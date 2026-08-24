package queue

import "github.com/redis/go-redis/v9"

// Processing tasks use a hash keyed by task id, not a list.
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

// acquireTaskScript moves the oldest ready task into the processing hashes. The
// pop and both index writes have to land together, or a crash between them loses
// either the task or its generation fence.
const acquireTaskScript = `
local ready = KEYS[1]
local processing = KEYS[2]
local leases = KEYS[3]
local lease_id = ARGV[1]

local encoded = redis.call("RPOP", ready)
if not encoded then
	return nil
end

local task = cjson.decode(encoded)
redis.call("HSET", processing, task["id"], encoded)
redis.call("HSET", leases, task["id"], lease_id)
return encoded
`

const completeTaskScript = `
local processing = KEYS[1]
local leases = KEYS[2]
local id = ARGV[1]
local lease_id = ARGV[2]

local encoded = redis.call("HGET", processing, id)
if not encoded then
	return 0
end
if redis.call("HGET", leases, id) ~= lease_id then
	return -1
end

redis.call("HDEL", processing, id)
redis.call("HDEL", leases, id)
return 1
`

const requeueTaskScript = `
local processing = KEYS[1]
local leases = KEYS[2]
local ready = KEYS[3]
local id = ARGV[1]
local lease_id = ARGV[2]

local encoded = redis.call("HGET", processing, id)
if not encoded then
	return 0
end
if redis.call("HGET", leases, id) ~= lease_id then
	return -1
end

redis.call("HDEL", processing, id)
redis.call("HDEL", leases, id)
local task = cjson.decode(encoded)
task["attempts"] = (task["attempts"] or 0) + 1
redis.call("LPUSH", ready, cjson.encode(task))
return 1
`

const recoverOrphanedProcessingScript = `
local processing = KEYS[1]
local leases = KEYS[2]
local ready = KEYS[3]

local active = {}
for i = 1, #ARGV, 2 do
	local id = ARGV[i]
	local lease_id = ARGV[i + 1]
	if id and lease_id then
		active[id .. "\0" .. lease_id] = true
	end
end

local entries = redis.call("HGETALL", processing)
local moved = 0
local matched = {}
for i = 1, #entries, 2 do
	local id = entries[i]
	local encoded = entries[i + 1]
	local lease_id = redis.call("HGET", leases, id)
	if lease_id and active[id .. "\0" .. lease_id] then
		table.insert(matched, lease_id)
	else
		redis.call("HDEL", processing, id)
		redis.call("HDEL", leases, id)
		redis.call("LPUSH", ready, encoded)
		moved = moved + 1
	end
end

local result = {moved}
for i = 1, #matched do
	table.insert(result, matched[i])
end
return result
`

const deadLetterTaskScript = `
local processing = KEYS[1]
local leases = KEYS[2]
local dead = KEYS[3]
local id = ARGV[1]
local lease_id = ARGV[2]
local reason = ARGV[3]
local dead_at = ARGV[4]

local encoded = redis.call("HGET", processing, id)
if not encoded then
	return 0
end
if redis.call("HGET", leases, id) ~= lease_id then
	return -1
end

redis.call("HDEL", processing, id)
redis.call("HDEL", leases, id)
local task = cjson.decode(encoded)
task["attempts"] = (task["attempts"] or 0) + 1
redis.call("LPUSH", dead, cjson.encode({
	task = task,
	reason = reason,
	dead_at = dead_at
}))
return 1
`
