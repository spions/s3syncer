-- ARGV: 3 id
local id = ARGV[3]
local tk = tkey(id)
if redis.call('EXISTS', tk) == 0 then
  redis.call('ZREM', PROCESSING, id)
  return 0
end
redis.call('ZREM', PROCESSING, id)
redis.call('ZREM', FAILED, id)
pending_add(id, tonumber(redis.call('HGET', tk, 'priority')), tonumber(redis.call('HGET', tk, 'created_ms')))
redis.call('HSET', tk, 'status', 'pending', 'dirty', '0', 'updated_ms', n(NOW))
return 1
