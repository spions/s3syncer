-- Shared prelude, prepended to every script.
-- KEYS: 1 pending (ZSET, priority-ordered)  2 pending_age (ZSET, by created_ms)
--       3 processing (ZSET, by last heartbeat)  4 failed (ZSET, by failure time)
-- ARGV: 1 clock offset in ms (always 0 in production, used by tests)  2 task hash key prefix
-- Time comes from Redis, never from clients, so instances with skewed clocks agree.
local PENDING, AGE, PROCESSING, FAILED = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local TASK_TTL_MS = 604800000 -- safety net: orphaned task hashes disappear after 7 days
local t = redis.call('TIME')
local NOW = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000) + tonumber(ARGV[1])

-- Numbers must be passed to Redis as plain integers, never as 1.7e+12.
local function n(x) return string.format('%.0f', x) end
local function tkey(id) return ARGV[2] .. id end
-- Lower score = served first: higher priority, then older.
local function score(prio, created) return n((2 - prio) * 1e13 + created) end

local function pending_add(id, prio, created)
  redis.call('ZADD', PENDING, score(prio, created), id)
  redis.call('ZADD', AGE, n(created), id)
end
local function pending_rem(id)
  redis.call('ZREM', PENDING, id)
  redis.call('ZREM', AGE, id)
end
