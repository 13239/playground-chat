package com.project.chat.auth.repository;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** MVC와 동일한 v1 정책 검사를 기존 WebSocket 인증 Lua와 한 원자 연산으로 실행한다. */
public final class AccessPolicyScripts {
    private AccessPolicyScripts() { }
    public static RedisScript<Long> script(String location, int sessionKey) {
        try {
            String common = new ClassPathResource("redis/access/gate.lua").getContentAsString(StandardCharsets.UTF_8);
            String guard = "\nlocal gate = access_session(KEYS[" + sessionKey + "]); if gate ~= 1 then return gate end\n";
            return RedisScript.of(common + guard + new ClassPathResource(location).getContentAsString(StandardCharsets.UTF_8), Long.class);
        } catch (IOException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }
}
