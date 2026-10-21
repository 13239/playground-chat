-- 등록된 유효한 연결에 대해서만 만료 시간을 연장한다. 만료된 연결의 소생은 금지한다.
-- KEYS: 사용자 인덱스, 해당 사용자 연결 ZSET, 공개 프로필 HASH, 변경 알림 채널
-- ARGV: userId, connectionId, 채팅 서버 현재 시각(ms), TTL(ms)
local now = tonumber(ARGV[3])
local previous = redis.call('ZSCORE', KEYS[2], ARGV[2])
if not previous or tonumber(previous) <= now then return 0 end
local expires = now + tonumber(ARGV[4])
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
redis.call('ZADD', KEYS[2], expires, ARGV[2])
local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[1], tonumber(latest[2]), ARGV[1])
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[4]) * 3)
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[4]) * 3)
if removed > 0 then redis.call('PUBLISH', KEYS[4], 'changed') end
return 1
