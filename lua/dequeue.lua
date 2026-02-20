-- dequeue.lua
-- Atomically moves a job from the ready queue to the processing sorted set,
-- updates the job hash status, and returns the full job data.
--
-- KEYS[1]: ready queue list (e.g., gqm:queue:default:ready)
-- KEYS[2]: processing sorted set (e.g., gqm:queue:default:processing)
-- KEYS[3]: job hash key prefix (e.g., gqm:job:)
-- KEYS[4]: paused set key (e.g., gqm:paused)
--
-- ARGV[1]: current timestamp (unix seconds)
-- ARGV[2]: global timeout (seconds) — fallback if no job/pool timeout
-- ARGV[3]: worker ID
-- ARGV[4]: queue name (to check against paused set)
-- ARGV[5]: pool timeout (seconds, 0 = not set)
--
-- Returns: array [job_id, field1, val1, field2, val2, ...] if dequeued,
--          nil if queue is empty or paused

-- Check if queue is paused before attempting dequeue.
if redis.call('SISMEMBER', KEYS[4], ARGV[4]) == 1 then
    return nil
end

local job_id = redis.call('RPOP', KEYS[1])
if not job_id then
    return nil
end

-- Guard: only process if job status is 'ready'. Prevents resurrecting
-- canceled/completed jobs whose ID ended up in the ready queue.
-- Also catches missing job hashes (HMGET returns false/nil for missing keys).
local job_key = KEYS[3] .. job_id
local fields = redis.call('HMGET', job_key, 'status', 'timeout')
if fields[1] ~= 'ready' then
    return nil
end

-- Resolve timeout hierarchy: job-level > pool-level > global.
local job_timeout = tonumber(fields[2]) or 0
local pool_timeout = tonumber(ARGV[5]) or 0
local timeout = tonumber(ARGV[2])
if job_timeout > 0 then
    timeout = job_timeout
elseif pool_timeout > 0 then
    timeout = pool_timeout
end
local deadline = tonumber(ARGV[1]) + timeout
redis.call('ZADD', KEYS[2], deadline, job_id)

redis.call('HSET', job_key,
    'status', 'processing',
    'started_at', ARGV[1],
    'worker_id', ARGV[3])

-- Return job_id followed by all hash fields (already includes updated status).
local data = redis.call('HGETALL', job_key)
table.insert(data, 1, job_id)
return data
