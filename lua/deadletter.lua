-- deadletter.lua
-- Moves a job to the dead letter queue: removes from processing set,
-- adds to DLQ sorted set, updates job hash.
--
-- KEYS[1]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[2]: dead letter sorted set (e.g., gqm:queue:default:dead_letter)
-- KEYS[3]: job hash key (e.g., gqm:job:{id})
--
-- ARGV[1]: job ID
-- ARGV[2]: current timestamp (unix seconds)
-- ARGV[3]: error message
--
-- Returns: 1 on success

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
redis.call('HSET', KEYS[3],
    'status', 'dead_letter',
    'error', ARGV[3],
    'completed_at', ARGV[2])

return 1
