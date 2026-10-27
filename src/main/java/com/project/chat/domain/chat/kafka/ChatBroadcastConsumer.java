package com.project.chat.domain.chat.kafka;

import com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.domain.chat.service.ChatOutboundHub;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.types.ObjectId;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** 서버 인스턴스마다 독립 consumer group으로 같은 저장 완료 이벤트를 받아 로컬 연결에 전파한다. */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChatBroadcastConsumer {

    private final ChatOutboundHub hub;
    private final JsonMapper jsonMapper;

    // 한 소비자가 이 인스턴스의 모든 방송 파티션을 맡아 전체 할당과 시작 위치를 확인한다.
    @KafkaListener(id = ChatBroadcastReadiness.LISTENER_ID, topics = "${chat.kafka.broadcasts-topic}",
            groupId = "#{@chatBroadcastGroupId}", properties = "auto.offset.reset=latest",
            containerFactory = "chatBroadcastListenerContainerFactory", concurrency = "1")
    public void forward(ConsumerRecord<String, String> record) {
        try {
            ChatBroadcastEvent event = jsonMapper.readValue(record.value(), ChatBroadcastEvent.class);
            if (event == null || event.getVersion() != 1 || event.getId() == null
                    || !ObjectId.isValid(event.getId()) || event.getAuthorId() == null
                    || event.getClientMessageId() == null || event.getCreatedAt() == null
                    || event.getContent() == null || event.getConversationKey() == null
                    // 공개 경계를 알 수 없는 이전 버전 방송은 새 로그인에 재노출하지 않는다.
                    || event.getAcceptedAtMicros() == null || event.getAcceptedAtMicros() <= 0) {
                throw new IllegalArgumentException("Invalid broadcast event");
            }
            String expectedKey = ChatDestination.builder().scope(event.getScope())
                    .gameType(event.getGameType()).roomId(event.getRoomId()).conversationId(event.getConversationId()).build().getConversationKey();
            if (!expectedKey.equals(event.getConversationKey()) || !expectedKey.equals(record.key())) {
                throw new IllegalArgumentException("Wrong conversation key");
            }
            hub.publish(event);
        } catch (RuntimeException exception) {
            log.warn("잘못된 채팅 전파 이벤트를 건너뜁니다. partition={}, offset={}", record.partition(), record.offset());
        }
    }
}
