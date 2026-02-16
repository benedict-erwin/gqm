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
--
-- Returns: 1 if canceled, 0 if not deferred (already promoted or canceled)

local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'deferred' then
    return 0
end

redis.call('HSET', KEYS[1], 'status', 'canceled')
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[3])
return 1
