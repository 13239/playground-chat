-- MVC와 chat이 공유하는 v1 세션 정책 계약. 기존 인증 Lua 앞에 결합해 같은 원자 연산으로 검사한다.
-- 버전은 19자리 0 채움 십진 문자열로 비교하여 Lua double의 정수 정밀도 제한을 피한다.
local policyKey = 'auth:access-policy:v1'
local function access_role(role, revision)
    local policy = redis.call('HMGET', policyKey, 'state', 'version', 'enabled', 'message')
    local state = policy[1]
    -- 부분 유실·손상은 제한 해제로 해석하지 않는다. 정상 동기화 전까지 인증을 보류한다.
    if (state ~= 'ready' and state ~= 'pending') or not policy[2] or string.len(policy[2]) ~= 19
        or not string.match(policy[2], '^%d+$') or (policy[3] ~= 'true' and policy[3] ~= 'false')
        or not policy[4] or policy[4] == '' then return -3 end
    if not role or not revision or string.len(revision) ~= 19 or not string.match(revision, '^%d+$') then return 0 end
    if revision > policy[2] then return -3 end
    if role == 'ROOT' then return 1 end
    if state ~= 'ready' then return -3 end
    if policy[3] == 'true'
        and redis.call('HGET', policyKey, 'allow:' .. role) ~= 'true' then return -2 end
    local cutoff = redis.call('HGET', policyKey, 'revoke:' .. role)
    if cutoff and revision < cutoff then return -2 end
    return 1
end
local function access_session(key)
    if redis.call('EXISTS', key) == 0 then return 0 end
    local fields = redis.call('HMGET', key, 'accessRole', 'accessRevision')
    return access_role(fields[1], fields[2])
end
