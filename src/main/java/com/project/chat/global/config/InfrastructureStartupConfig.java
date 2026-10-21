package com.project.chat.global.config;

import com.project.chat.global.support.KafkaHealthIndicator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** MongoDB·Redis·Kafka를 시작 필수 조건으로 확인하고 실패 시 요청 수신 전에 종료한다. */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class InfrastructureStartupConfig {

    private static final Duration CHECK_TIMEOUT = Duration.ofSeconds(5);

    @Bean
    public SmartInitializingSingleton infrastructureStartupVerifier(
            ReactiveMongoTemplate mongoTemplate,
            ReactiveStringRedisTemplate redisTemplate,
            KafkaHealthIndicator kafkaHealthIndicator) {
        return () -> {
            Mono<Void> mongo = mongoTemplate.executeCommand("{ ping: 1 }")
                    .filter(result -> result.get("ok") instanceof Number ok && ok.doubleValue() == 1.0)
                    .switchIfEmpty(Mono.error(new IllegalStateException("MongoDB PING 응답이 올바르지 않습니다.")))
                    .then();
            Mono<Void> redis = redisTemplate.execute(connection -> connection.ping())
                    .single()
                    .filter("PONG"::equals)
                    .switchIfEmpty(Mono.error(new IllegalStateException("Redis PING 응답이 올바르지 않습니다.")))
                    .then();
            Mono<Void> kafka = kafkaHealthIndicator.clusterId().then();

            // 기동 스레드에서만 대기한다. 요청 처리와 WebSocket 이벤트 루프에서는 block을 사용하지 않는다.
            Mono.when(
                    verify("MongoDB", mongo),
                    verify("Redis", redis),
                    verify("Kafka", kafka)
            ).block(CHECK_TIMEOUT.plusSeconds(1));
            log.info("MongoDB·Redis·Kafka 연결 확인 완료");
        };
    }

    private Mono<Void> verify(String dependency, Mono<Void> check) {
        return check.timeout(CHECK_TIMEOUT).onErrorMap(exception -> new IllegalStateException(
                dependency + " 연결 확인에 실패했습니다. 실행 상태와 접속 설정을 확인해 주세요.", exception));
    }
}
