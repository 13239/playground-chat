package com.project.chat.domain.adminchat;

import com.project.chat.domain.adminchat.dto.AdminChatHealth;
import com.project.chat.domain.chat.entity.ChatMessage;
import com.project.chat.domain.chat.kafka.ChatBroadcastReadiness;
import com.project.chat.global.support.KafkaHealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.time.*;

/** WebFlux 스레드를 막지 않고 저장소 읽기·세션 저장소·브로커·방송 준비 상태를 확인한다. */
@Service
public class AdminChatHealthService {
    private final Mono<AdminChatHealth> cached;
    public AdminChatHealthService(ReactiveMongoTemplate mongo, ReactiveStringRedisTemplate redis,
            KafkaHealthIndicator kafka, ChatBroadcastReadiness broadcast) {
        // back 인스턴스/관리자 수에 비례해 점검이 늘지 않도록 짧게 공유하며 실패도 상태로 변환한다.
        cached = Mono.defer(() -> {
            long started = System.nanoTime();
            Mono<Boolean> storage = mongo.exists(new Query().maxTime(Duration.ofSeconds(2)), ChatMessage.class).thenReturn(true);
            Mono<Boolean> sessions = redis.execute(connection -> connection.ping()).single().map("PONG"::equals);
            Mono<Boolean> broker = kafka.health().map(health -> Status.UP.equals(health.getStatus()));
            Mono<Boolean> ready = broadcast.health().map(health -> Status.UP.equals(health.getStatus()));
            return Mono.zip(storage, sessions, broker, ready).timeout(Duration.ofSeconds(4))
                    .map(values -> status(values.getT1() && values.getT2() && values.getT3() && values.getT4()
                            ? System.nanoTime() - started >= 1_000_000_000L ? "DELAYED" : "NORMAL" : "DOWN"))
                    .onErrorResume(java.util.concurrent.TimeoutException.class, ignored -> Mono.just(status("UNKNOWN")))
                    .onErrorResume(ignored -> Mono.just(status("DOWN")));
        }).cache(Duration.ofSeconds(10));
    }
    public Mono<AdminChatHealth> status() { return cached; }
    private AdminChatHealth status(String state) { return AdminChatHealth.builder().state(state).checkedAt(Instant.now()).build(); }
}
