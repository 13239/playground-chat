-- 종료된 한 탭의 연결만 삭제한다. 다른 탭이 남아 있다면 사용자는 온라인 상태를 유지한다.
-- KEYS: 사용자 리스, 해당 사용자 연결 ZSET, 공개 프로필 HASH, 변경 채널, 고정 정렬 인덱스
-- ARGV: userId, connectionId
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local previous = redis.call('ZSCORE', KEYS[1], ARGV[1])
local wasOnline = previous and tonumber(previous) > now
-- 다른 사용자의 만료 항목도 고정 정렬 인덱스와 함께 최대 128명씩 정리한다.
local expired = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE', 'LIMIT', 0, 128)
for _, user in ipairs(expired) do redis.call('ZREM', KEYS[1], user); redis.call('ZREM', KEYS[5], user) end
local removed = #expired
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if #latest == 0 then
    redis.call('ZREM', KEYS[1], ARGV[1])
    redis.call('ZREM', KEYS[5], ARGV[1])
    redis.call('DEL', KEYS[2], KEYS[3])
    if wasOnline or removed > 0 then redis.call('PUBLISH', KEYS[4], 'changed') end
else
    redis.call('ZADD', KEYS[1], tonumber(latest[2]), ARGV[1])
    if removed > 0 then redis.call('PUBLISH', KEYS[4], 'changed') end
end
return 1
