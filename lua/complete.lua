-- complete.lua
-- Marks a job as completed: removes from processing set,
-- adds to completed set, updates job hash.
--
-- KEYS[1]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[2]: completed sorted set (e.g., gqm:queue:default:completed)
-- KEYS[3]: job hash key (e.g., gqm:job:{id})
--
-- ARGV[1]: job ID
-- ARGV[2]: current timestamp (unix seconds)
-- ARGV[3]: result JSON (may be empty string)
-- ARGV[4]: execution duration (milliseconds)
--
-- Returns: 1 on success, 0 if job was not in processing set

if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
    return 0
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
redis.call('HSET', KEYS[3],
    'status', 'completed',
    'completed_at', ARGV[2],
    'execution_duration', ARGV[4])

if ARGV[3] ~= '' then
    redis.call('HSET', KEYS[3], 'result', ARGV[3])
end

return 1
