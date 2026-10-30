-- 리스 시각과 고정 순서를 분리한다. KEYS: 사용자 리스, 고정 정렬 / ARGV: 이전 사용자 키(내부 전용), 크기, 인증된 조회자 키
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local size = tonumber(ARGV[2])
local lower = ARGV[1] == '' and '-' or '(' .. ARGV[1]
-- 만료 잔여 항목이 많아도 한 요청에서 최대 256명만 검사해 Redis 실행 시간을 제한한다.
local candidates = redis.call('ZRANGE', KEYS[2], lower, '+', 'BYLEX', 'LIMIT', 0, 256)
local users = {}
local viewer = ARGV[3]
-- 본인이 원래 첫 페이지 범위 밖에 있어도 유효한 리스가 있을 때만 맨 위에 한 번 표시한다.
local ownExpiry = tonumber(redis.call('ZSCORE', KEYS[1], viewer))
if ARGV[1] == '' and ownExpiry and ownExpiry > now then table.insert(users, viewer) end
local scanned = ''
-- 양의 회원 번호보다 앞선 내부 경계다. 크기가 1이면 본인 다음 페이지도 첫 상대부터 조회한다.
local lastReturned = ARGV[1] == '' and '0' or ARGV[1]
local hasMore = false
for _, user in ipairs(candidates) do
    scanned = user
    local expiry = tonumber(redis.call('ZSCORE', KEYS[1], user))
    -- 본인은 첫 페이지에서 별도로 표시했으므로 이어 읽기에서 중복하지 않는다.
    if user ~= viewer and expiry and expiry > now then
        if #users == size then hasMore = true; break end
        table.insert(users, user)
        lastReturned = user
    end
end
local next = ''
if hasMore then
    next = lastReturned
elseif #candidates == 256 then
    next = scanned
end
local result = {tostring(redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf')), next}
for _, user in ipairs(users) do table.insert(result, user) end
return result
