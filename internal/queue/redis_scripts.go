package queue

import "github.com/redis/go-redis/v9"

type redisQueueScripts struct {
	complete *redis.Script
	requeue  *redis.Script
}

func defaultRedisQueueScripts() redisQueueScripts {
	return redisQueueScripts{
		complete: redis.NewScript(completeTaskScript),
		requeue:  redis.NewScript(requeueTaskScript),
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
		redis.call("LPUSH", ready, encoded)
		return 1
	end
end

return 0
`
