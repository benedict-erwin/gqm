-- admin_delete.lua
-- Atomically delete a job: check status, remove from location, delete keys.
-- Prevents TOCTOU race where a job transitions to "processing" between check and delete.
--
-- KEYS[1]: job hash (gqm:job:{id})
-- KEYS[2]: ready list (gqm:queue:{name}:ready)
-- KEYS[3]: scheduled sorted set (gqm:scheduled)
-- KEYS[4]: deferred set (gqm:deferred)
-- KEYS[5]: completed sorted set (gqm:queue:{name}:completed)
-- KEYS[6]: dead_letter sorted set (gqm:queue:{name}:dead_letter)
-- KEYS[7]: deps set (gqm:job:{id}:deps)
-- KEYS[8]: pending_deps set (gqm:job:{id}:pending_deps)
-- KEYS[9]: dependents set (gqm:job:{id}:dependents)
--
-- ARGV[1]: job ID
--
-- Returns: 1 on success, -1 if job not found, -2 if job is processing

local status = redis.call('HGET', KEYS[1], 'status')
if not status then
    return -1
end

if status == 'processing' then
    return -2
end

-- Remove from current location based on status
if status == 'ready' then
    redis.call('LREM', KEYS[2], 1, ARGV[1])
elseif status == 'scheduled' then
    redis.call('ZREM', KEYS[3], ARGV[1])
elseif status == 'deferred' then
    redis.call('SREM', KEYS[4], ARGV[1])
elseif status == 'completed' then
    redis.call('ZREM', KEYS[5], ARGV[1])
elseif status == 'dead_letter' then
    redis.call('ZREM', KEYS[6], ARGV[1])
end
-- canceled, stopped, failed: only have the hash, no separate set

-- Delete job hash and DAG keys
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[7])
redis.call('DEL', KEYS[8])
redis.call('DEL', KEYS[9])

return 1
