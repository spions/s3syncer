-- KEYS: 1 index (ZSET by created_ms)  2 running (ZSET by last heartbeat ms)
-- ARGV: 1 clock offset ms (0 in production, tests only)  2 task hash key prefix
local INDEX, RUNNING = KEYS[1], KEYS[2]
local KEEP_MS = 2592000000 -- 30 days safety TTL for task hashes
local t = redis.call('TIME')
local NOW = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000) + tonumber(ARGV[1])
local function n(x) return string.format('%.0f', x) end
local function tkey(id) return ARGV[2] .. id end
-- Frees the "one running task per type+bucket" slot if this task still holds it.
local function release_slot(tk, id)
  local ak = redis.call('HGET', tk, 'active_key')
  if ak and redis.call('GET', ak) == id then redis.call('DEL', ak) end
end
