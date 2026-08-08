-- reap.lua
-- Reclaims a job whose processing deadline has passed and whose worker process
-- is presumed dead: either schedules a retry or moves it to the dead letter
-- queue. Unlike retry.lua and deadletter.lua, this script is safe to call from
-- an instance that never claimed the job.
--
-- KEYS[1]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[2]: scheduled sorted set (e.g., gqm:scheduled)
-- KEYS[3]: job hash key (e.g., gqm:job:{id})
-- KEYS[4]: dead letter sorted set (e.g., gqm:queue:default:dead_letter)
-- KEYS[5]: dependents set key (e.g., gqm:job:{id}:dependents)
-- KEYS[6]: daily stats key (e.g., gqm:stats:default:failed:2026-01-01)
-- KEYS[7]: total stats key (e.g., gqm:stats:default:failed_total)
--
-- ARGV[1]: job ID
-- ARGV[2]: staleness threshold (unix seconds) the caller scanned with
-- ARGV[3]: action, 'retry' or 'deadletter'
-- ARGV[4]: current timestamp (unix seconds)
-- ARGV[5]: error message
-- ARGV[6]: retry timestamp (unix seconds) — 'retry' only
-- ARGV[7]: new retry count — 'retry' only
-- ARGV[8]: daily stats TTL (seconds) — 'deadletter' only
-- ARGV[9]: failure retention TTL (seconds) — 'deadletter' only: >0 sets an
--          expiry on the job hash, 0 deletes the record outright, <0 retains it
--          permanently
--
-- Returns: 0 if the job already left processing or was re-claimed,
--          1 on retry,
--          2 on dead letter (no dependents),
--          3 on dead letter (has dependents — caller should propagate failure)

-- Guard: skip if the job already left processing OR was re-claimed. Between the
-- caller's scan and this call the job may have completed, been re-enqueued and
-- re-dequeued by another instance; that re-claim writes a fresh deadline score,
-- necessarily above the threshold that was scanned with. The plain ZREM == 0
-- guard used by retry.lua and deadletter.lua cannot tell a re-claim from a
-- still-abandoned job, so it would steal the new claim and run the job twice.
--
-- Passing this guard also means the job hash has not moved on since the caller
-- read it — every exit from processing removes the job from this set — so the
-- retry-or-dead-letter decision computed there is still the right one.
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then
    return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])

-- Retry: mirrors retry.lua.
if ARGV[3] == 'retry' then
    redis.call('ZADD', KEYS[2], tonumber(ARGV[6]), ARGV[1])
    redis.call('HSET', KEYS[3],
        'status', 'retry',
        'error', ARGV[5],
        'retry_count', tonumber(ARGV[7]))
    return 1
end

-- Dead letter: mirrors deadletter.lua.
local retention = tonumber(ARGV[9])

-- See complete.lua: a DLQ entry whose job hash is gone is worse than no entry,
-- because the dashboard would list a job it cannot open and admin retry would
-- fail on it.
if retention ~= 0 then
    redis.call('ZADD', KEYS[4], tonumber(ARGV[4]), ARGV[1])
end

-- Trim DLQ entries whose job hash has already expired, by score so the same
-- retention number governs hash and set alike.
if retention > 0 then
    redis.call('ZREMRANGEBYSCORE', KEYS[4], 0, tonumber(ARGV[4]) - retention)
end
redis.call('HSET', KEYS[3],
    'status', 'dead_letter',
    'error', ARGV[5],
    'completed_at', ARGV[4])

-- Stats: increment failed counters.
redis.call('INCR', KEYS[6])
redis.call('EXPIRE', KEYS[6], tonumber(ARGV[8]))
redis.call('INCR', KEYS[7])

-- Retention: terminal state reached, so the record may now expire.
if retention == 0 then
    redis.call('DEL', KEYS[3])
elseif retention > 0 then
    redis.call('EXPIRE', KEYS[3], retention)
end

-- Check if this job has dependents waiting on it (DAG).
if redis.call('EXISTS', KEYS[5]) == 1 then
    return 3
end

return 2
