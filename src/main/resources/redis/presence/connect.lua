-- 한 사용자에게 여러 탭이 있어도 전역 사용자 인덱스에는 단일 항목만 저장한다.
-- KEYS: 사용자 리스, 해당 사용자의 연결 ZSET, 공개 프로필 HASH, 변경 채널, 고정 정렬 인덱스
-- ARGV: userId, connectionId, TTL(ms), nickname, avatarId, 연결 수 상한
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local expires = now + tonumber(ARGV[3])
local previous = redis.call('ZSCORE', KEYS[1], ARGV[1])
local wasOnline = previous and tonumber(previous) > now
-- 만료 정리는 한 번에 128명으로 제한해 대규모 비정상 종료가 Redis를 장시간 점유하지 않게 한다.
local expired = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE', 'LIMIT', 0, 128)
for _, user in ipairs(expired) do redis.call('ZREM', KEYS[1], user); redis.call('ZREM', KEYS[5], user) end
local removed = #expired
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if not redis.call('ZSCORE', KEYS[2], ARGV[2]) and redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[6]) then
    return 0
end
redis.call('ZADD', KEYS[2], expires, ARGV[2])
local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[1], tonumber(latest[2]), ARGV[1])
-- 모든 정렬 점수는 0이다. 갱신·퇴장으로 위치가 밀리지 않는 사용자 키 순서를 별도 유지한다.
redis.call('ZADD', KEYS[5], 0, ARGV[1])
redis.call('HSET', KEYS[3], 'nickname', ARGV[4], 'avatarId', ARGV[5])
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]) * 3)
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[3]) * 3)
if removed > 0 or not wasOnline then
    redis.call('PUBLISH', KEYS[4], 'changed')
end
return 1
