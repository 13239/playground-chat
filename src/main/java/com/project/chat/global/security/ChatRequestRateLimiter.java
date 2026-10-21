package com.project.chat.global.security;

import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/** 인증된 사용자 기준으로 전송과 조회 예산을 분리하고 조회 종류끼리는 같은 예산을 공유한다. */
@Component
@RequiredArgsConstructor
public class ChatRequestRateLimiter {
    private static final RedisScript<Long> ACQUIRE = RedisScript.of(
            new ClassPathResource("redis/chat/request-limit.lua"), Long.class);
    private final ReactiveStringRedisTemplate redis;

    public Mono<Long> send(long userId) {
        // 연결당 200ms 제한에 더해 사용자 전체를 초당 5개·버스트 5개로 제한한다.
        return acquire(userId, "send", 5, 5);
    }

    public Mono<Long> read(long userId) {
        // 이력·접속자 목록·전송 상태 조회가 탭 수나 요청 종류로 예산을 우회하지 못하게 한다.
        return acquire(userId, "read", 4, 2);
    }

    private Mono<Long> acquire(long userId, String operation, int capacity, int rate) {
        return redis.execute(ACQUIRE, List.of("chat:limit:{user:" + userId + "}:" + operation),
                        List.of(Integer.toString(capacity), Integer.toString(rate)))
                .single().timeout(Duration.ofSeconds(3))
                .onErrorMap(exception -> new BusinessException(ErrorCode.CHAT_SERVICE_UNAVAILABLE))
                .flatMap(result -> result > 0 ? Mono.just(result)
                        : Mono.error(new ChatRequestLimitException(-result)));
    }
}
