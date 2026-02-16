-- schedule_poll.lua
-- Polls the scheduled sorted set for jobs that are ready to be enqueued.
-- Moves them from scheduled set to their respective ready queues.
--
-- KEYS[1]: scheduled sorted set (e.g., gqm:scheduled)
-- KEYS[2]: job hash key prefix (e.g., gqm:job:)
-- KEYS[3]: queue key prefix (e.g., gqm:queue:)
--
-- ARGV[1]: current timestamp (unix seconds)
-- ARGV[2]: batch size
--
-- Returns: number of jobs moved

local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0

for _, job_id in ipairs(jobs) do
    -- Only proceed if ZREM actually removed the job (prevents duplicate enqueue).
    if redis.call('ZREM', KEYS[1], job_id) == 1 then
        -- Skip if job hash does not exist (prevents creating zombie records).
        if redis.call('EXISTS', KEYS[2] .. job_id) == 1 then
            -- Guard: only move jobs in scheduled/retry status to prevent
            -- overwriting status of completed/canceled/processing jobs.
            local status = redis.call('HGET', KEYS[2] .. job_id, 'status')
            if status == 'scheduled' or status == 'retry' then
                local queue = redis.call('HGET', KEYS[2] .. job_id, 'queue')
                if queue and queue ~= false then
                    local ready_key = KEYS[3] .. queue .. ':ready'
                    redis.call('LPUSH', ready_key, job_id)
                    redis.call('HSET', KEYS[2] .. job_id, 'status', 'ready')
                    moved = moved + 1
                else
                    -- Missing queue field: default to "default" to prevent silent job loss.
                    local ready_key = KEYS[3] .. 'default:ready'
                    redis.call('LPUSH', ready_key, job_id)
                    redis.call('HSET', KEYS[2] .. job_id, 'status', 'ready', 'queue', 'default')
                    moved = moved + 1
                end
            end
        end
    end
end

return moved
