package com.project.chat.auth.service;

import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import java.time.Duration;

/** 프로세스당 공유 구독 하나로 운영 변경을 즉시 알린다. 유실은 기존 주기 검증으로 보완한다. */
@Component
public class ChatAccessPolicyChanges {
    private final Flux<Long> changes;
    public ChatAccessPolicyChanges(ReactiveRedisMessageListenerContainer listener) {
        changes = listener.receive(ChannelTopic.of("auth:access-policy:changed:v1")).map(ignored -> 0L)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(10)))
                .publish().refCount(1);
    }
    public Flux<Long> changes() { return changes; }
}
