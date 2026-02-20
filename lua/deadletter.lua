-- deadletter.lua
-- Moves a job to the dead letter queue: removes from processing set,
-- adds to DLQ sorted set, updates job hash, increments stats counters.
--
-- KEYS[1]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[2]: dead letter sorted set (e.g., gqm:queue:default:dead_letter)
-- KEYS[3]: job hash key (e.g., gqm:job:{id})
-- KEYS[4]: dependents set key (e.g., gqm:job:{id}:dependents)
-- KEYS[5]: daily stats key (e.g., gqm:stats:default:failed:2026-01-01)
-- KEYS[6]: total stats key (e.g., gqm:stats:default:failed_total)
--
-- ARGV[1]: job ID
-- ARGV[2]: current timestamp (unix seconds)
-- ARGV[3]: error message
-- ARGV[4]: daily stats TTL (seconds)
--
-- Returns: 0 if job was not in processing set,
--          1 on success (no dependents),
--          2 on success (has dependents — caller should propagate failure)

if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
    return 0
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
redis.call('HSET', KEYS[3],
    'status', 'dead_letter',
    'error', ARGV[3],
    'completed_at', ARGV[2])

-- Stats: increment failed counters.
redis.call('INCR', KEYS[5])
redis.call('EXPIRE', KEYS[5], tonumber(ARGV[4]))
redis.call('INCR', KEYS[6])

-- Check if this job has dependents waiting on it (DAG).
if redis.call('EXISTS', KEYS[4]) == 1 then
    return 2
end

return 1
