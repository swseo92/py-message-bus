-- ack_and_dedup.lua: Atomically dedup-check and XACK a stream message.
-- If duplicate (key already exists): XACK + return 0
-- If first time: SET NX EX + return 1  (caller must XACK after processing)
--
-- KEYS[1]: dedup_key
-- KEYS[2]: stream_key
-- KEYS[3]: consumer_group
-- ARGV[1]: message_id
-- ARGV[2]: ttl (seconds)
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[2]))
if not ok then
  redis.call('XACK', KEYS[2], KEYS[3], ARGV[1])
  return 0
end
return 1
