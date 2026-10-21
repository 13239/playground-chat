-- 등록된 유효한 연결에 대해서만 만료 시간을 연장한다. 만료된 연결의 소생은 금지한다.
-- KEYS: 사용자 리스, 해당 사용자 연결 ZSET, 공개 프로필 HASH, 변경 채널, 고정 정렬 인덱스
-- ARGV: userId, connectionId, TTL(ms)
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local previous = redis.call('ZSCORE', KEYS[2], ARGV[2])
if not previous or tonumber(previous) <= now then return 0 end
local expires = now + tonumber(ARGV[3])
-- 리스와 정렬 인덱스를 같은 원자적 실행에서 제한된 개수만 정리한다.
local expired = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE', 'LIMIT', 0, 128)
for _, user in ipairs(expired) do redis.call('ZREM', KEYS[1], user); redis.call('ZREM', KEYS[5], user) end
local removed = #expired
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
redis.call('ZADD', KEYS[2], expires, ARGV[2])
local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[1], tonumber(latest[2]), ARGV[1])
redis.call('ZADD', KEYS[5], 0, ARGV[1])
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]) * 3)
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[3]) * 3)
if removed > 0 then redis.call('PUBLISH', KEYS[4], 'changed') end
return 1
