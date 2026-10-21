-- 리스 시각과 고정 순서를 분리한다. KEYS: 사용자 리스, 고정 정렬 / ARGV: 이전 사용자 키(내부 전용), 크기
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local size = tonumber(ARGV[2])
local lower = ARGV[1] == '' and '-' or '(' .. ARGV[1]
-- 만료 잔여 항목이 많아도 한 요청에서 최대 256명만 검사해 Redis 실행 시간을 제한한다.
local candidates = redis.call('ZRANGE', KEYS[2], lower, '+', 'BYLEX', 'LIMIT', 0, 256)
local users = {}
local scanned = ''
for _, user in ipairs(candidates) do
    scanned = user
    local expiry = tonumber(redis.call('ZSCORE', KEYS[1], user))
    if expiry and expiry > now then
        table.insert(users, user)
        if #users > size then break end
    end
end
local next = ''
if #users > size then
    table.remove(users)
    next = users[#users]
elseif #candidates == 256 then
    next = scanned
end
local result = {tostring(redis.call('ZCOUNT', KEYS[1], '(' .. now, '+inf')), next}
for _, user in ipairs(users) do table.insert(result, user) end
return result
