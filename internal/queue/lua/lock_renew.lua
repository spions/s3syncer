-- KEYS[1] lock key; ARGV: 1 token  2 ttl_ms.  Extends the lock only if we still own it.
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
