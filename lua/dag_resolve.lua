-- dag_resolve.lua
-- Atomically resolves a dependency for a deferred job.
-- Removes the completed parent from the pending_deps set, checks if all
-- dependencies are met, and if so moves the job to the ready queue.
--
-- KEYS[1]: pending_deps set (e.g., gqm:job:{dependent}:pending_deps)
-- KEYS[2]: ready queue list (e.g., gqm:queue:{name}:ready)
-- KEYS[3]: job hash key (e.g., gqm:job:{dependent})
-- KEYS[4]: deferred global set (e.g., gqm:deferred)
--
-- ARGV[1]: parent job ID (the one that just completed)
-- ARGV[2]: dependent job ID
-- ARGV[3]: enqueue_at_front flag ("1" = RPUSH to front, otherwise LPUSH to back)
--
-- Returns: 1 if job was promoted to ready, 0 if still waiting or not deferred

-- Guard: only proceed if job is still deferred (prevents resurrecting canceled jobs).
local status = redis.call('HGET', KEYS[3], 'status')
if status ~= 'deferred' then
    return 0
end

redis.call('SREM', KEYS[1], ARGV[1])
local remaining = redis.call('SCARD', KEYS[1])

if remaining == 0 then
    -- All dependencies met: promote to ready queue.
    if ARGV[3] == '1' then
        redis.call('RPUSH', KEYS[2], ARGV[2])
    else
        redis.call('LPUSH', KEYS[2], ARGV[2])
    end
    redis.call('HSET', KEYS[3], 'status', 'ready')
    redis.call('SREM', KEYS[4], ARGV[2])
    redis.call('DEL', KEYS[1])
    return 1
end

return 0
