-- ARGV: 3 interval_ms  4 max_age_ms  5 batch.  Returns number moved to pending.
local interval, max_age, batch = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local ids = redis.call('ZRANGEBYSCORE', FAILED, '-inf', n(NOW - interval), 'LIMIT', 0, batch)
local moved = 0
for _, id in ipairs(ids) do
  local tk = tkey(id)
  local f = redis.call('HMGET', tk, 'priority', 'created_ms', 'src')
  if not f[1] then
    redis.call('ZREM', FAILED, id) -- orphan
  elseif NOW - tonumber(f[2]) < max_age or (f[3] and string.sub(f[3], 1, 7) == 'DELETE:') then
    redis.call('ZREM', FAILED, id)
    pending_add(id, tonumber(f[1]), tonumber(f[2]))
    redis.call('HSET', tk, 'status', 'pending', 'updated_ms', n(NOW))
    moved = moved + 1
  end -- too old: left for purge_expired
end
return moved
