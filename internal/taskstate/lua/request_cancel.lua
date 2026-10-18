-- ARGV: 3 id.  Returns 1 if the task is running and the cancel flag was set.
local tk = tkey(ARGV[3])
if redis.call('HGET', tk, 'status') ~= 'running' then return 0 end
redis.call('HSET', tk, 'cancel', '1')
return 1
