-- ARGV: 3 id  4 bucket  5 key  6 src  7 tgt  8 priority
local id, prio = ARGV[3], tonumber(ARGV[8])
local tk = tkey(id)

if redis.call('EXISTS', tk) == 0 then
  redis.call('HSET', tk, 'bucket', ARGV[4], 'key', ARGV[5], 'src', ARGV[6], 'tgt', ARGV[7],
    'priority', n(prio), 'status', 'pending', 'attempts', '0', 'last_error', '', 'dirty', '0',
    'created_ms', n(NOW), 'updated_ms', n(NOW))
  pending_add(id, prio, NOW)
  redis.call('PEXPIRE', tk, TASK_TTL_MS)
  return 1
end

local status = redis.call('HGET', tk, 'status')
local cur = tonumber(redis.call('HGET', tk, 'priority'))
if prio > cur then
  redis.call('HSET', tk, 'priority', n(prio))
  if status == 'pending' then
    pending_add(id, prio, tonumber(redis.call('HGET', tk, 'created_ms')))
  end
end
if status == 'processing' then
  -- changed while being copied: MarkDone must send it around again
  redis.call('HSET', tk, 'dirty', '1')
end
redis.call('PEXPIRE', tk, TASK_TTL_MS)
return 0
