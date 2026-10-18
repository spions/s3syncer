-- KEYS[1] lock key; ARGV: 1 token.  Deletes the lock only if we still own it.
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
