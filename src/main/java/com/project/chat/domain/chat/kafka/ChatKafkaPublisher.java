package com.project.chat.domain.chat.kafka;

import com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent;
import com.project.chat.domain.chat.dto.internal.ChatSendCommand;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.global.config.ChatKafkaProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

/** Kafka broker의 확인 응답을 받은 뒤에만 WebSocket에 접수 완료를 알린다. */
@Component
@RequiredArgsConstructor
public class ChatKafkaPublisher {

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(8);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ChatKafkaProperties properties;
    private final JsonMapper jsonMapper;

    public Mono<Void> enqueue(ChatSendCommand command) {
        return Mono.defer(() -> send(properties.getMessagesTopic(),
                ChatDestination.builder().scope(command.getScope())
                        .gameType(command.getGameType()).roomId(command.getRoomId())
                        .build().getConversationKey(), jsonMapper.writeValueAsString(command)));
    }

    public Mono<Void> broadcast(ChatBroadcastEvent event) {
        return Mono.defer(() -> send(properties.getBroadcastsTopic(), event.getConversationKey(),
                jsonMapper.writeValueAsString(event)));
    }

    private Mono<Void> send(String topic, String key, String payload) {
        // KafkaTemplate은 비동기다. 대기 시간을 제한하되 완료 여부가 불확실하면 클라이언트의 동일 UUID 재시도를 허용한다.
        return Mono.fromCallable(() -> kafkaTemplate.send(topic, key, payload))
                // KafkaProducer.send는 메타데이터 조회 중 block할 수 있으므로 Netty 이벤트 루프에서 분리한다.
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(Mono::fromFuture)
                .timeout(SEND_TIMEOUT)
                .then();
    }
}
