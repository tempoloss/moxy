package queue

import "github.com/redis/go-redis/v9"

type redisQueueScripts struct {
	complete *redis.Script
	requeue  *redis.Script
	dead     *redis.Script
}

func defaultRedisQueueScripts() redisQueueScripts {
	return redisQueueScripts{
		complete: redis.NewScript(completeTaskScript),
		requeue:  redis.NewScript(requeueTaskScript),
		dead:     redis.NewScript(deadLetterTaskScript),
	}
}

const completeTaskScript = `
local processing = KEYS[1]
local needle = ARGV[1]
local tasks = redis.call("LRANGE", processing, 0, -1)

for _, encoded in ipairs(tasks) do
	if string.find(encoded, needle, 1, true) then
		local removed = redis.call("LREM", processing, 1, encoded)
		if removed == 0 then
			return 0
		end
		return 1
	end
end

return 0
`

const requeueTaskScript = `
local processing = KEYS[1]
local ready = KEYS[2]
local needle = ARGV[1]
local tasks = redis.call("LRANGE", processing, 0, -1)

for _, encoded in ipairs(tasks) do
	if string.find(encoded, needle, 1, true) then
		local removed = redis.call("LREM", processing, 1, encoded)
		if removed == 0 then
			return 0
		end
		local task = cjson.decode(encoded)
		task["attempts"] = (task["attempts"] or 0) + 1
		redis.call("LPUSH", ready, cjson.encode(task))
		return 1
	end
end

return 0
`

const deadLetterTaskScript = `
local processing = KEYS[1]
local dead = KEYS[2]
local needle = ARGV[1]
local reason = ARGV[2]
local dead_at = ARGV[3]
local tasks = redis.call("LRANGE", processing, 0, -1)

for _, encoded in ipairs(tasks) do
	if string.find(encoded, needle, 1, true) then
		local removed = redis.call("LREM", processing, 1, encoded)
		if removed == 0 then
			return 0
		end
		local task = cjson.decode(encoded)
		task["attempts"] = (task["attempts"] or 0) + 1
		local dead_task = {
			task = task,
			reason = reason,
			dead_at = dead_at
		}
		redis.call("LPUSH", dead, cjson.encode(dead_task))
		return 1
	end
end

return 0
`
