-- back의 find.lua와 같은 로그인 필수 필드·소유자·만료·목록·계정 차단을 조회만 검증한다.
-- KEYS: 로그인 Hash, 사용자 세션 ZSET, 계정 차단 / ARGV: sid, userId, 브라우저 키 지문
if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
local fields = redis.call('HMGET', KEYS[1], 'sessionId', 'id', 'username', 'refreshToken',
    'keepLoggedIn', 'createdAt', 'expiresAt', 'browserKeyThumbprint')
for i = 1, 8 do if not fields[i] then return 0 end end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if fields[1] ~= ARGV[1] or fields[2] ~= ARGV[2] or fields[8] ~= ARGV[3]
    or not tonumber(fields[7]) or tonumber(fields[7]) <= now
    or not redis.call('ZSCORE', KEYS[2], ARGV[1] .. '|' .. fields[7]) then return 0 end
return 1
