-- ARGV: 3 limit.  Returns {id, bucket, key, src, tgt, priority, attempts, last_error, created_ms}[]
local limit = tonumber(ARGV[3])
local ids = redis.call('ZRANGE', PENDING, 0, limit - 1)
local out = {}
for _, id in ipairs(ids) do
  pending_rem(id)
  local tk = tkey(id)
  local f = redis.call('HMGET', tk, 'bucket', 'key', 'src', 'tgt', 'priority', 'attempts', 'last_error', 'created_ms')
  if f[1] then -- a missing hash is an orphan: dropped from pending above
    redis.call('ZADD', PROCESSING, n(NOW), id)
    redis.call('HSET', tk, 'status', 'processing', 'dirty', '0', 'updated_ms', n(NOW))
    redis.call('PEXPIRE', tk, TASK_TTL_MS)
    out[#out + 1] = {id, f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]}
  end
end
return out
