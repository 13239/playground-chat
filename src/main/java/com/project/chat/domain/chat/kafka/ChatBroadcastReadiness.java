package com.project.chat.domain.chat.kafka;

import com.project.chat.global.config.ChatKafkaProperties;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.context.event.EventListener;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppingEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Collection;
import java.util.stream.Collectors;

/** 방송 파티션 전체의 시작 위치가 확정된 실행 구간에만 새 연결과 기존 연결을 허용한다. */
@Component("chatBroadcast")
public class ChatBroadcastReadiness implements ReactiveHealthIndicator {

    public static final String LISTENER_ID = "chat-local-broadcasts";
    private static final Duration POSITION_TIMEOUT = Duration.ofSeconds(5);
    private static final Health READY = Health.up().build();
    private static final Health NOT_READY = Health.outOfService().build();
    private final String topic;
    private volatile Sinks.Empty<Void> active;

    public ChatBroadcastReadiness(ChatKafkaProperties properties) {
        this.topic = properties.getBroadcastsTopic();
    }

    @Override
    public Mono<Health> health() {
        // readiness도 같은 메모리 상태만 읽으며 추가 Kafka 조회나 내부 연결 정보를 만들지 않는다.
        return Mono.fromSupplier(() -> active == null ? NOT_READY : READY);
    }

    /** 요청 스레드에서는 Kafka를 조회하거나 준비를 기다리지 않고 즉시 승인·거절한다. */
    public Mono<Void> requireReady() {
        return Mono.defer(() -> active == null ? Mono.error(unavailable()) : Mono.empty());
    }

    /** 재할당·중단을 놓친 기존 연결은 종료시켜 재접속 시 이력으로 다시 동기화한다. */
    public Mono<Void> unavailableSignal() {
        return Mono.defer(() -> {
            Sinks.Empty<Void> current = active;
            return current == null ? Mono.error(unavailable())
                    : current.asMono().then(Mono.error(unavailable()));
        });
    }

    /** Boot가 모든 리스너에 자동 적용하지 않도록 리밸런스 리스너 자체는 전역 Bean으로 등록하지 않는다. */
    public ConsumerAwareRebalanceListener rebalanceListener() {
        return new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                initializePositions(consumer);
            }

            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                markUnavailable();
            }

            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                markUnavailable();
            }
        };
    }

    private void initializePositions(Consumer<?, ?> consumer) {
        markUnavailable();
        long startedAt = System.nanoTime();
        // 증분 할당이나 실제 토픽의 추가 파티션도 확인한다. 설정의 파티션 수만으로 준비를 판단하지 않는다.
        try {
            var expected = consumer.partitionsFor(topic, remaining(startedAt)).stream()
                    .map(partition -> new TopicPartition(topic, partition.partition())).collect(Collectors.toSet());
            if (expected.isEmpty() || !consumer.assignment().containsAll(expected)) return;
            // latest는 위치 조회 시 처음 확정될 수 있다. 모든 위치를 확정한 뒤에만 접속을 허용한다.
            // 재할당에서는 저장된 위치를 유지하며 seekToEnd로 미처리 방송을 건너뛰지 않는다.
            for (TopicPartition partition : expected) consumer.position(partition, remaining(startedAt));
            active = Sinks.empty();
        } catch (RuntimeException exception) {
            // 일시적인 위치 조회 실패 후 할당 콜백이 다시 오지 않아 영구 미준비 상태로 남지 않게 한다.
            consumer.enforceRebalance("Chat broadcast position initialization failed");
            throw exception;
        }
    }

    @EventListener({ConsumerStartingEvent.class, ConsumerStoppingEvent.class, ConsumerStoppedEvent.class})
    public void onConsumerLifecycle(KafkaEvent event) {
        // 저장 소비자의 수명주기는 방송 준비 상태를 바꾸지 않는다. 자식 이벤트는 부모 리스너 ID로 구분한다.
        if (LISTENER_ID.equals(event.getContainer(MessageListenerContainer.class).getListenerId())) markUnavailable();
    }

    private void markUnavailable() {
        Sinks.Empty<Void> previous = active;
        active = null;
        if (previous != null) previous.tryEmitEmpty();
    }

    private static Duration remaining(long startedAt) {
        long nanos = POSITION_TIMEOUT.toNanos() - (System.nanoTime() - startedAt);
        if (nanos <= 0) throw new TimeoutException("채팅 방송 시작 위치 확인 시간이 초과되었습니다.");
        return Duration.ofNanos(nanos);
    }

    private static BusinessException unavailable() {
        return new BusinessException(ErrorCode.CHAT_SERVICE_UNAVAILABLE);
    }
}
