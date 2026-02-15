-- retry.lua
-- Moves a job from processing to retry state: removes from processing set,
-- increments retry count, sets retry status, and adds to scheduled set
-- for delayed retry.
--
-- KEYS[1]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[2]: scheduled sorted set (e.g., gqm:scheduled)
-- KEYS[3]: job hash key (e.g., gqm:job:{id})
--
-- ARGV[1]: job ID
-- ARGV[2]: retry timestamp (when to retry, unix seconds)
-- ARGV[3]: error message
-- ARGV[4]: new retry count
--
-- Returns: 1 on success

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
redis.call('HSET', KEYS[3],
    'status', 'retry',
    'error', ARGV[3],
    'retry_count', tonumber(ARGV[4]))

return 1
