package queue

import "github.com/redis/go-redis/v9"

// Processing tasks are held in a hash keyed by task id rather than a list.
//
// A list forces every transition to scan: the earlier implementation pulled the
// whole processing list into Lua with LRANGE and substring-matched each entry
// for the task id. That is O(n) per ack, requeue and dead-letter, it runs inside
// a script that blocks the entire Redis server for its duration, and matching
// encoded JSON by substring quietly depended on Go's field ordering. A hash
// makes every one of those transitions a single O(1) field lookup.
type redisQueueScripts struct {
	acquire        *redis.Script
	complete       *redis.Script
	requeue        *redis.Script
	dead           *redis.Script
	recoverOrphans *redis.Script
}

func defaultRedisQueueScripts() redisQueueScripts {
	return redisQueueScripts{
		acquire:        redis.NewScript(acquireTaskScript),
		complete:       redis.NewScript(completeTaskScript),
		requeue:        redis.NewScript(requeueTaskScript),
		dead:           redis.NewScript(deadLetterTaskScript),
		recoverOrphans: redis.NewScript(recoverOrphanedProcessingScript),
	}
}

// acquireTaskScript moves the oldest ready task into the processing hash. The
// pop and the index write have to land together, or a crash between them loses
// the task entirely.
const acquireTaskScript = `
local ready = KEYS[1]
local processing = KEYS[2]

local encoded = redis.call("RPOP", ready)
if not encoded then
	return nil
end

local task = cjson.decode(encoded)
redis.call("HSET", processing, task["id"], encoded)
return encoded
`

const completeTaskScript = `
local processing = KEYS[1]
local id = ARGV[1]

if redis.call("HDEL", processing, id) == 0 then
	return 0
end
return 1
`

const requeueTaskScript = `
local processing = KEYS[1]
local ready = KEYS[2]
local id = ARGV[1]

local encoded = redis.call("HGET", processing, id)
if not encoded then
	return 0
end

redis.call("HDEL", processing, id)
local task = cjson.decode(encoded)
task["attempts"] = (task["attempts"] or 0) + 1
redis.call("LPUSH", ready, cjson.encode(task))
return 1
`

const recoverOrphanedProcessingScript = `
local processing = KEYS[1]
local ready = KEYS[2]

local active = {}
for i = 1, #ARGV do
	active[ARGV[i]] = true
end

local entries = redis.call("HGETALL", processing)
local moved = 0
for i = 1, #entries, 2 do
	local id = entries[i]
	local encoded = entries[i + 1]
	if not active[id] then
		redis.call("HDEL", processing, id)
		redis.call("LPUSH", ready, encoded)
		moved = moved + 1
	end
end

return moved
`

const deadLetterTaskScript = `
local processing = KEYS[1]
local dead = KEYS[2]
local id = ARGV[1]
local reason = ARGV[2]
local dead_at = ARGV[3]

local encoded = redis.call("HGET", processing, id)
if not encoded then
	return 0
end

redis.call("HDEL", processing, id)
local task = cjson.decode(encoded)
task["attempts"] = (task["attempts"] or 0) + 1
redis.call("LPUSH", dead, cjson.encode({
	task = task,
	reason = reason,
	dead_at = dead_at
}))
return 1
`
