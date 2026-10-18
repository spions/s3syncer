-- ARGV: 3 older_than_ms  4 reason  5 batch.  Returns the number of tasks failed.
local older, batch = tonumber(ARGV[3]), tonumber(ARGV[5])
local ids = redis.call('ZRANGEBYSCORE', RUNNING, '-inf', n(NOW - older), 'LIMIT', 0, batch)
local failed = 0
for _, id in ipairs(ids) do
  redis.call('ZREM', RUNNING, id)
  local tk = tkey(id)
  if redis.call('HGET', tk, 'status') == 'running' then
    redis.call('HSET', tk, 'status', 'failed', 'error', ARGV[4], 'updated_ms', n(NOW))
    release_slot(tk, id)
    failed = failed + 1
  end
end
return failed
