-- 집계도 리스 갱신과 같은 Redis 시계를 사용한다. KEYS[1]: 사용자 리스 ZSET
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
return redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf')
