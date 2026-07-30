-- complete.lua
-- Marks a job as completed: removes from processing set,
-- adds to completed set, updates job hash, increments stats counters.
--
-- KEYS[1]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[2]: completed sorted set (e.g., gqm:queue:default:completed)
-- KEYS[3]: job hash key (e.g., gqm:job:{id})
-- KEYS[4]: dependents set key (e.g., gqm:job:{id}:dependents)
-- KEYS[5]: daily stats key (e.g., gqm:stats:default:processed:2026-01-01)
-- KEYS[6]: total stats key (e.g., gqm:stats:default:processed_total)
--
-- ARGV[1]: job ID
-- ARGV[2]: current timestamp (unix seconds)
-- ARGV[3]: result JSON (may be empty string)
-- ARGV[4]: execution duration (milliseconds)
-- ARGV[5]: daily stats TTL (seconds)
-- ARGV[6]: result retention TTL (seconds): >0 sets an expiry on the job hash,
--          0 deletes the record outright, <0 retains it permanently
--
-- Returns: 0 if job was not in processing set,
--          1 on success (no dependents),
--          2 on success (has dependents — caller should resolve DAG)

if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
    return 0
end

local retention = tonumber(ARGV[6])

-- With zero retention the completed record is not kept at all, so the zset
-- entry is skipped too: an entry whose job hash is gone would make the
-- dashboard page a job it can no longer show.
if retention ~= 0 then
    redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
end

-- Trim entries whose job hash has already expired. The score is the completion
-- timestamp and the hash expiry uses the same window, so trimming by score
-- keeps the two in lockstep -- no entry survives its own job's details. Done by
-- score rather than by rank so a single retention number governs both, and
-- inline rather than in a sweeper because the set is only ever grown here.
if retention > 0 then
    redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, tonumber(ARGV[2]) - retention)
end
redis.call('HSET', KEYS[3],
    'status', 'completed',
    'completed_at', ARGV[2],
    'execution_duration', ARGV[4])

-- Clear error from previous failed attempts (if any).
redis.call('HDEL', KEYS[3], 'error')

if ARGV[3] ~= '' then
    redis.call('HSET', KEYS[3], 'result', ARGV[3])
end

-- Stats: increment processed counters.
redis.call('INCR', KEYS[5])
redis.call('EXPIRE', KEYS[5], tonumber(ARGV[5]))
redis.call('INCR', KEYS[6])

-- Retention: the job has reached a terminal state, so its record may now
-- expire. Only terminal jobs get an expiry — a pending or processing job that
-- expired mid-flight would be lost work, which is far worse than unbounded
-- growth. KEYS[4] is deliberately left alone: the Go caller deletes the
-- dependents set once the DAG is resolved.
if retention == 0 then
    redis.call('DEL', KEYS[3])
elseif retention > 0 then
    redis.call('EXPIRE', KEYS[3], retention)
end

-- Check if this job has dependents waiting on it (DAG).
if redis.call('EXISTS', KEYS[4]) == 1 then
    return 2
end

return 1
