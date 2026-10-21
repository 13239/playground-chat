-- 한 사용자의 모든 탭·세션·서버가 같은 토큰 버킷을 사용한다. KEYS[1]: 사용자/작업별 버킷
-- ARGV: 용량, 초당 보충량. 허용 시 Redis 마이크로초 시각, 거절 시 음수 재시도 대기(ms)를 반환한다.
local clock = redis.call('TIME')
local micros = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
local now = math.floor(micros / 1000)
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local tokens = math.min(capacity, (tonumber(state[1]) or capacity)
    + math.max(0, now - (tonumber(state[2]) or now)) * rate / 1000)
local wait = 0
if tokens >= 1 then tokens = tokens - 1 else wait = math.max(1, math.ceil((1 - tokens) * 1000 / rate)) end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 1000 / rate) + 1000)
if wait > 0 then return -wait end
return micros
