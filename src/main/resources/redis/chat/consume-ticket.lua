-- 원문 증명은 저장하지 않는다. 브라우저 키 확인 후 세션 재검증과 증명 삭제를 한 번에 수행한다.
-- KEYS: 증명 Hash, 세션별 대기 ZSET, 로그인 Hash, 사용자 세션 ZSET, 계정 차단
-- ARGV: sid, userId, 브라우저 키 지문, Origin, WebSocket URL, 검증 시 읽은 홈 채팅 공개 경계
local grant = redis.call('HMGET', KEYS[1], 'sessionId', 'userId', 'keyThumbprint', 'origin', 'websocketUrl', 'expiresAt', 'chatStartedAtMicros')
for i = 1, 7 do if not grant[i] then return 0 end end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if grant[1] ~= ARGV[1] or grant[2] ~= ARGV[2] or grant[3] ~= ARGV[3]
    or grant[4] ~= ARGV[4] or grant[5] ~= ARGV[5] or grant[7] ~= ARGV[6]
    or not tonumber(grant[6]) or tonumber(grant[6]) <= now then return 0 end

-- 발급 대기 목록에서도 같은 만료 시각으로 등록된 증명만 소비한다.
local hash = string.sub(KEYS[1], string.len('chat:auth:ticket:') + 1)
local pendingExpiry = tonumber(redis.call('ZSCORE', KEYS[2], hash))
if not pendingExpiry or pendingExpiry ~= tonumber(grant[6]) then return 0 end

local session = redis.call('HMGET', KEYS[3], 'sessionId', 'id', 'username', 'refreshToken',
    'keepLoggedIn', 'createdAt', 'expiresAt', 'browserKeyThumbprint', 'chatStartedAtMicros')
local valid = redis.call('EXISTS', KEYS[5]) == 0
for i = 1, 9 do if not session[i] then valid = false end end
if valid then
    valid = session[1] == ARGV[1] and session[2] == ARGV[2] and session[8] == ARGV[3]
        -- 티켓에 담긴 공개 경계도 로그인 세션의 원본과 일치해야 한다.
        and session[9] == grant[7] and tonumber(grant[7]) ~= nil and tonumber(grant[7]) > 0
        and tonumber(session[7]) ~= nil and tonumber(session[7]) > now
        and redis.call('ZSCORE', KEYS[4], ARGV[1] .. '|' .. session[7]) ~= false
end

-- 동시 요청 중 이 삭제를 먼저 수행한 한 연결만 성공한다. 로그인 자체는 변경하지 않는다.
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], hash)
if redis.call('ZCARD', KEYS[2]) == 0 then redis.call('DEL', KEYS[2]) end
if not valid then return -1 end
return 1
