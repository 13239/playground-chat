package com.project.chat.domain.chat.service;

import com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.global.security.ChatPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.BufferOverflowStrategy;

/** Kafka broadcast 토픽에서 받은 메시지를 현재 JVM의 인증된 WebSocket 연결에만 전달한다. */
@Component
@RequiredArgsConstructor
public class ChatOutboundHub {

    private static final int CONNECTION_BUFFER = 128;
    // 느린 연결이 다른 연결을 지연시키지 않도록 직접 전달하고 연결별 버퍼 초과 시 해당 연결을 종료한다.
    private final Sinks.Many<ChatBroadcastEvent> broadcasts = Sinks.many().multicast().directBestEffort();
    private final ChatDestinationAccessPolicy accessPolicy;

    public synchronized void publish(ChatBroadcastEvent event) {
        // 서로 다른 Kafka 파티션 소비자가 동시에 발행해도 sink의 비직렬화 실패로 이벤트를 잃지 않는다.
        Sinks.EmitResult result = broadcasts.tryEmitNext(event);
        if (result != Sinks.EmitResult.OK && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER
                && result != Sinks.EmitResult.FAIL_OVERFLOW) {
            throw new IllegalStateException("채팅 로컬 전파에 실패했습니다: " + result);
        }
        // 수신자가 없거나 전원이 느린 경우 저장 이력이 진실의 원천이다. 새 연결은 이력을 재조회한다.
    }

    public Flux<ChatBroadcastEvent> subscribe(ChatPrincipal principal, ChatDestination destination) {
        return accessPolicy.authorize(principal, destination)
                .thenMany(broadcasts.asFlux()
                        .filter(event -> event.getConversationKey().equals(destination.getConversationKey()))
                        .onBackpressureBuffer(CONNECTION_BUFFER, dropped -> { }, BufferOverflowStrategy.ERROR));
    }
}
