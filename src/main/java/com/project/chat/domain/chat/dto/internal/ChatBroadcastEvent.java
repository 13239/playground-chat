package com.project.chat.domain.chat.dto.internal;

import com.project.chat.domain.chat.dto.response.ChatMessageResponse;
import com.project.chat.domain.chat.entity.ChatMessage;
import com.project.chat.domain.chat.enums.ChatGameType;
import com.project.chat.domain.chat.enums.ChatScope;
import com.project.chat.domain.chat.support.ChatMessageTime;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;

/** MongoDB 저장이 확인된 메시지의 내부 전파 이벤트다. 작성자 ID는 브라우저에 노출하지 않는다. */
@Getter
@Builder
@Jacksonized
public class ChatBroadcastEvent {
    private final int version;
    private final String id;
    private final String clientMessageId;
    private final String conversationKey;
    private final ChatScope scope;
    private final ChatGameType gameType;
    private final Long roomId;
    private final String conversationId;
    private final Long authorId;
    private final String nickname;
    private final String avatarId;
    private final String content;
    private final Instant createdAt;
    // 서버 간 방송도 이 값을 사용해 로그인 전 대화를 걸러낸다.
    private final Long acceptedAtMicros;

    public static ChatBroadcastEvent from(ChatMessage message) {
        return ChatBroadcastEvent.builder()
                .version(1)
                .id(message.getId().toHexString())
                .clientMessageId(message.getClientMessageId())
                .conversationKey(message.getConversationKey())
                .scope(message.getScope())
                .gameType(message.getGameType())
                .roomId(message.getRoomId())
                .conversationId(message.getConversationId())
                .authorId(message.getAuthorId())
                .nickname(message.getAuthorNickname())
                .avatarId(message.getAuthorAvatarId())
                .content(message.getContent())
                // DB 재조회로 밀리초가 절삭돼도 최초 방송과 재전파의 표시 시각을 같게 유지한다.
                .createdAt(ChatMessageTime.fromAcceptedAtMicros(message.getAcceptedAtMicros(), message.getCreatedAt()))
                .acceptedAtMicros(message.getAcceptedAtMicros())
                .build();
    }

    public ChatMessageResponse toViewer(Long viewerId) {
        boolean own = authorId.equals(viewerId);
        return ChatMessageResponse.builder()
                .id(id)
                // 발신자에게만 요청 식별자를 돌려주어 전송 중 상태를 완료 상태로 바꾼다.
                .clientMessageId(own ? clientMessageId : null)
                .scope(scope)
                .gameType(gameType)
                .roomId(roomId)
                .conversationId(conversationId)
                .nickname(nickname)
                .avatarId(avatarId)
                .content(content)
                // 이미 Kafka에 대기 중인 구버전 이벤트도 최초 접수 시각으로 정규화한다.
                .createdAt(ChatMessageTime.fromAcceptedAtMicros(acceptedAtMicros, createdAt))
                .ownMessage(own)
                .build();
    }
}
