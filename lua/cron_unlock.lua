-- cron_unlock.lua
-- Atomically releases a cron lock only if still owned by the caller.
-- Prevents TOCTOU race between GET and DEL.
--
-- KEYS[1]: lock key (e.g., gqm:cron:lock:{entry_id})
--
-- ARGV[1]: expected owner (instance ID)
--
-- Returns: 1 if deleted, 0 if lock was not owned

if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
