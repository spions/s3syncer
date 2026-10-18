-- ARGV: 3 id  4 status  5 error.  Returns 1 if it was running and is now finished.
local id = ARGV[3]
local tk = tkey(id)
if redis.call('HGET', tk, 'status') ~= 'running' then return 0 end
redis.call('HSET', tk, 'status', ARGV[4], 'error', ARGV[5], 'updated_ms', n(NOW))
redis.call('ZREM', RUNNING, id)
release_slot(tk, id)
return 1
