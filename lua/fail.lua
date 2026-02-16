-- fail.lua
-- Reserved for future manual-fail API (e.g., admin dashboard).
-- Currently not called from Go code; all failures go through retry.lua
-- or deadletter.lua via handleFailure.
--
-- Marks a job as failed: removes from processing set,
-- adds to failed set, updates job hash.
--
-- KEYS[1]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[2]: failed sorted set (e.g., gqm:queue:default:failed)
-- KEYS[3]: job hash key (e.g., gqm:job:{id})
--
-- ARGV[1]: job ID
-- ARGV[2]: current timestamp (unix seconds)
-- ARGV[3]: error message
-- ARGV[4]: execution duration (milliseconds)
--
-- Returns: 1 on success, 0 if job was not in processing set

if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
    return 0
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
redis.call('HSET', KEYS[3],
    'status', 'failed',
    'completed_at', ARGV[2],
    'error', ARGV[3],
    'execution_duration', ARGV[4])

return 1
