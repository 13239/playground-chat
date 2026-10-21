-- 업그레이드 전에 실제 접속 IP의 발급 속도와 인증 대기 연결 수를 함께 제한한다.
-- KEYS: IP 버킷, IP 대기 리스 ZSET / ARGV: 리스 UUID, 최대 대기, 리스(ms), 버스트, 초당 보충
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local capacity = tonumber(ARGV[4])
local rate = tonumber(ARGV[5])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local tokens = math.min(capacity, (tonumber(state[1]) or capacity)
    + math.max(0, now - (tonumber(state[2]) or now)) * rate / 1000)
redis.call('HSET', KEYS[1], 'tokens', math.max(0, tokens - 1), 'updated', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 1000 / rate) + 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if tokens < 1 or redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]))
return 1
