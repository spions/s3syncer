-- ARGV: 3 max_age_ms  4 start  5 batch.  Returns {deleted, scanned}.
-- Failed tasks are ordered by failure time, not creation time, so the caller
-- walks the set in windows instead of assuming old tasks sit at the head.
local max_age, start, batch = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local ids = redis.call('ZRANGE', FAILED, start, start + batch - 1)
local deleted = 0
for _, id in ipairs(ids) do
  local tk = tkey(id)
  local created = redis.call('HGET', tk, 'created_ms')
  local src = redis.call('HGET', tk, 'src')
  -- delete tasks are kept until they succeed (see DeletePrefix)
  if src and string.sub(src, 1, 7) == 'DELETE:' then
    -- keep
  elseif not created or NOW - tonumber(created) > max_age then
    redis.call('ZREM', FAILED, id)
    redis.call('DEL', tk)
    deleted = deleted + 1
  end
end
return {deleted, #ids}
