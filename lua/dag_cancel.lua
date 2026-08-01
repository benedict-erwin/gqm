-- dag_cancel.lua
-- Atomically cancels a deferred job due to parent failure propagation.
-- Only cancels if the job is still in "deferred" status to prevent
-- overwriting a "ready" status set by a concurrent dag_resolve.
--
-- KEYS[1]: job hash key (e.g., gqm:job:{dependent})
-- KEYS[2]: deferred global set (e.g., gqm:deferred)
-- KEYS[3]: pending_deps set (e.g., gqm:job:{dependent}:pending_deps)
--
-- ARGV[1]: dependent job ID
-- ARGV[2]: current timestamp (unix seconds)
-- ARGV[3]: failure retention TTL (seconds): >0 sets an expiry on the job hash,
--          0 deletes the record outright, <0 retains it permanently
--
-- Returns: 1 if canceled, 0 if not deferred (already promoted or canceled)

local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'deferred' then
    return 0
end

redis.call('HSET', KEYS[1],
    'status', 'canceled',
    'completed_at', ARGV[2])
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[3])

-- Canceled is terminal and, unlike completed and dead-lettered jobs, is added
-- to no sorted set. An unexpired hash here is garbage nothing references: it
-- appears in no listing, counts toward no statistic, and no other mechanism
-- will ever collect it. Retention has to be applied here or not at all.
local retention = tonumber(ARGV[3])
if retention == 0 then
    redis.call('DEL', KEYS[1])
elseif retention > 0 then
    redis.call('EXPIRE', KEYS[1], retention)
end

return 1
