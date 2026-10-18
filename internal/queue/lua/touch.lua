-- ARGV: 3.. ids.  Heartbeat: only tasks that are still processing are refreshed.
local touched = 0
for i = 3, #ARGV do
  local id = ARGV[i]
  if redis.call('ZSCORE', PROCESSING, id) then
    redis.call('ZADD', PROCESSING, 'XX', n(NOW), id)
    redis.call('HSET', tkey(id), 'updated_ms', n(NOW))
    touched = touched + 1
  end
end
return touched
