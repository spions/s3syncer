-- ARGV: 3 threshold_ms  4 batch.  Returns number returned to pending.
local threshold, batch = tonumber(ARGV[3]), tonumber(ARGV[4])
local ids = redis.call('ZRANGEBYSCORE', PROCESSING, '-inf', '(' .. n(NOW - threshold), 'LIMIT', 0, batch)
local reset = 0
for _, id in ipairs(ids) do
  redis.call('ZREM', PROCESSING, id)
  local tk = tkey(id)
  local f = redis.call('HMGET', tk, 'priority', 'created_ms')
  if f[1] then
    pending_add(id, tonumber(f[1]), tonumber(f[2]))
    redis.call('HSET', tk, 'status', 'pending', 'dirty', '0', 'updated_ms', n(NOW))
    reset = reset + 1
  end
end
return reset
