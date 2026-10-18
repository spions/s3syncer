-- ARGV: 3 id  4 error message  5 backoff base ms  6 backoff cap ms
local id = ARGV[3]
local tk = tkey(id)
redis.call('ZREM', PROCESSING, id)
pending_rem(id)
if redis.call('EXISTS', tk) == 0 then return 0 end
local attempts = redis.call('HINCRBY', tk, 'attempts', 1)
-- The score of the failed set is the moment the task becomes retryable, minus the base
-- interval (retry_failed selects score <= NOW - interval). That keeps the set ordered by
-- "due first", so tasks still waiting out a long backoff never hide due ones behind them.
local base, cap = tonumber(ARGV[5]), tonumber(ARGV[6])
local wait = math.min(base * 2 ^ math.min(math.max(attempts - 1, 0), 20), cap)
redis.call('ZADD', FAILED, n(NOW + wait - base), id)
redis.call('HSET', tk, 'status', 'failed', 'last_error', ARGV[4], 'dirty', '0', 'updated_ms', n(NOW))
redis.call('PEXPIRE', tk, TASK_TTL_MS)
return 1
