package com.project.chat.domain.chat.kafka;

import com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent;
import com.project.chat.domain.chat.dto.internal.ChatSendCommand;
import com.project.chat.domain.chat.service.ChatMessageService;
import com.project.chat.global.exception.BusinessException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

/** 한 Kafka 파티션 안에서 저장 → 전파 발행 → offset 커밋 순서를 보장한다. */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChatMessageProcessor {

    private final ChatMessageService messageService;
    private final ChatKafkaPublisher publisher;
    private final JsonMapper jsonMapper;

    @KafkaListener(id = "chat-message-store", topics = "${chat.kafka.messages-topic}",
            groupId = "playground-chat-store-v1", concurrency = "${chat.kafka.partitions:3}")
    public void process(ConsumerRecord<String, String> record) {
        ChatSendCommand command;
        try {
            command = jsonMapper.readValue(record.value(), ChatSendCommand.class);
            if (command == null) throw new IllegalArgumentException("null command");
        } catch (RuntimeException exception) {
            // 서비스 전용 Kafka 토픽에 유입된 손상된 이벤트는 로그만 남기고 다음 이벤트를 처리한다.
            log.warn("잘못된 채팅 저장 이벤트를 건너뜁니다. partition={}, offset={}", record.partition(), record.offset());
            return;
        }
        try {
            // Kafka 리스너 전용 스레드만 블로킹한다. Netty WebFlux 이벤트 루프에서는 블로킹하지 않는다.
            messageService.saveAccepted(command)
                    .map(ChatBroadcastEvent::from)
                    .flatMap(publisher::broadcast)
                    .block(Duration.ofSeconds(20));
        } catch (BusinessException exception) {
            // 도메인 검증 실패나 중복 UUID 내용 충돌은 재시도해도 성공하지 않으므로 영구 실패 처리한다.
            log.warn("채팅 저장 이벤트가 거절됐습니다. code={}, partition={}, offset={}",
                    exception.getErrorCode().name(), record.partition(), record.offset());
        }
        // 일시적 DB/Kafka 장애는 예외를 Kafka 컨테이너에 전달하여 같은 offset부터 재처리한다.
    }
}
