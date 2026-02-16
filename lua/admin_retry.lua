-- admin_retry.lua
-- Retry a job from DLQ or failed: remove from source sorted set,
-- reset status, push to ready queue.
--
-- KEYS[1]: DLQ sorted set (gqm:queue:{name}:dead_letter)
-- KEYS[2]: ready list (gqm:queue:{name}:ready)
-- KEYS[3]: job hash (gqm:job:{id})
--
-- ARGV[1]: job ID
-- ARGV[2]: current timestamp
--
-- Returns: 1 on success, 0 if job not found in DLQ

local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
    return 0
end

redis.call('HSET', KEYS[3],
    'status', 'ready',
    'error', '',
    'retry_count', '0',
    'completed_at', '',
    'started_at', '',
    'worker_id', '',
    'execution_duration', '0')
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
