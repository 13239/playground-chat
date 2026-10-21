package com.project.chat.global.support;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** TCP 포트 유무가 아니라 Kafka 관리 요청의 성공 여부로 준비 상태를 판정한다. */
@Component
@RequiredArgsConstructor
public class KafkaHealthIndicator implements ReactiveHealthIndicator {

    private final Admin chatKafkaAdminClient;

    public Mono<String> clusterId() {
        // Admin의 비동기 Future를 연결하고 이벤트 루프에서 get/join/block을 호출하지 않는다.
        return Mono.fromCompletionStage(() -> chatKafkaAdminClient
                        .describeCluster(new DescribeClusterOptions().timeoutMs(3000))
                        .clusterId().toCompletionStage())
                .filter(id -> !id.isBlank())
                .switchIfEmpty(Mono.error(new IllegalStateException("Kafka 클러스터 식별자가 없습니다.")))
                .timeout(Duration.ofSeconds(5));
    }

    @Override
    public Mono<Health> health() {
        // 외부 상태 응답에는 브로커 주소·예외 메시지를 포함하지 않는다.
        return clusterId()
                .map(ignored -> Health.up().build())
                .onErrorResume(exception -> Mono.just(Health.down().build()));
    }
}
