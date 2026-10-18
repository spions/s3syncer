-- ARGV: 3 id  4 processed  5 total  6 chunk; processed = -1 means heartbeat only.
local id = ARGV[3]
local tk = tkey(id)
if redis.call('HGET', tk, 'status') ~= 'running' then return 0 end
if tonumber(ARGV[4]) >= 0 then
  redis.call('HSET', tk, 'processed', ARGV[4], 'total', ARGV[5], 'chunk', ARGV[6])
end
redis.call('HSET', tk, 'updated_ms', n(NOW))
redis.call('ZADD', RUNNING, n(NOW), id)
return 1
