-- admin_cancel.lua
-- Cancel a job: remove from its current location, mark as canceled.
--
-- KEYS[1]: job hash (gqm:job:{id})
-- KEYS[2]: ready list (gqm:queue:{name}:ready)
-- KEYS[3]: scheduled sorted set (gqm:scheduled)
-- KEYS[4]: deferred set (gqm:deferred)
--
-- ARGV[1]: job ID
-- ARGV[2]: current timestamp
-- ARGV[3]: current status (from Go caller)
--
-- Returns: 1 on success, 0 if job was not found in expected location

local status = ARGV[3]
local removed = 0

if status == 'ready' then
    removed = redis.call('LREM', KEYS[2], 1, ARGV[1])
elseif status == 'scheduled' then
    removed = redis.call('ZREM', KEYS[3], ARGV[1])
elseif status == 'deferred' then
    removed = redis.call('SREM', KEYS[4], ARGV[1])
else
    return 0
end

if removed == 0 then
    return 0
end

redis.call('HSET', KEYS[1],
    'status', 'canceled',
    'completed_at', ARGV[2])
return 1
