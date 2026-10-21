-- 한 사용자에게 여러 탭이 있어도 전역 사용자 인덱스에는 단일 항목만 저장한다.
-- KEYS: 사용자 인덱스, 해당 사용자의 연결 ZSET, 공개 프로필 HASH, 변경 알림 채널
-- ARGV: userId, connectionId, 채팅 서버 현재 시각(ms), TTL(ms), nickname, avatarId, 연결 수 상한
local now = tonumber(ARGV[3])
local expires = now + tonumber(ARGV[4])
local previous = redis.call('ZSCORE', KEYS[1], ARGV[1])
local wasOnline = previous and tonumber(previous) > now
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if not redis.call('ZSCORE', KEYS[2], ARGV[2]) and redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[7]) then
    return 0
end
redis.call('ZADD', KEYS[2], expires, ARGV[2])
local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[1], tonumber(latest[2]), ARGV[1])
redis.call('HSET', KEYS[3], 'nickname', ARGV[5], 'avatarId', ARGV[6])
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[4]) * 3)
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[4]) * 3)
if removed > 0 or not wasOnline then
    redis.call('PUBLISH', KEYS[4], 'changed')
end
return 1
