-- ARGV: 3 id.  Returns 0 unknown, 1 deleted, 2 requeued (was dirty)
local id = ARGV[3]
local tk = tkey(id)
redis.call('ZREM', PROCESSING, id)
if redis.call('EXISTS', tk) == 0 then
  pending_rem(id)
  redis.call('ZREM', FAILED, id)
  return 0
end
if redis.call('HGET', tk, 'dirty') == '1' and redis.call('HGET', tk, 'status') == 'processing' then
  pending_add(id, tonumber(redis.call('HGET', tk, 'priority')), tonumber(redis.call('HGET', tk, 'created_ms')))
  redis.call('HSET', tk, 'status', 'pending', 'dirty', '0', 'updated_ms', n(NOW))
  return 2
end
pending_rem(id)
redis.call('ZREM', FAILED, id)
redis.call('DEL', tk)
return 1
