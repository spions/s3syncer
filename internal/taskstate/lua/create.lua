-- ARGV: 3 id  4 type  5 bucket  6 owner  7 active key (one per type+bucket).
-- Returns 1 on success, 0 if the id exists, or the id (string) of the already running task.
local tk = tkey(ARGV[3])
if redis.call('EXISTS', tk) == 1 then return 0 end
local ak = ARGV[7]
local cur = redis.call('GET', ak)
if cur and redis.call('HGET', tkey(cur), 'status') == 'running' then return cur end
redis.call('HSET', tk, 'id', ARGV[3], 'type', ARGV[4], 'bucket', ARGV[5], 'owner', ARGV[6],
  'active_key', ak, 'status', 'running', 'total', '0', 'processed', '0', 'chunk', '0', 'error', '', 'cancel', '0',
  'created_ms', n(NOW), 'updated_ms', n(NOW))
redis.call('ZADD', INDEX, n(NOW), ARGV[3])
redis.call('ZADD', RUNNING, n(NOW), ARGV[3])
redis.call('SET', ak, ARGV[3], 'PX', KEEP_MS)
redis.call('PEXPIRE', tk, KEEP_MS)
return 1
