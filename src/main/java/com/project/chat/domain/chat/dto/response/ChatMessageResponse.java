package com.project.chat.domain.chat.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.project.chat.domain.chat.entity.ChatMessage;
import com.project.chat.domain.chat.enums.ChatGameType;
import com.project.chat.domain.chat.enums.ChatScope;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

/** 저장된 메시지만 외부에 제공하며 내부 사용자 ID·로그인 세션·브라우저 키는 노출하지 않는다. */
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatMessageResponse {

    private final String id;
    private final String clientMessageId;
    private final ChatScope scope;
    private final ChatGameType gameType;
    private final Long roomId;
    private final String nickname;
    private final String avatarId;
    private final String content;
    private final Instant createdAt;
    private final boolean ownMessage;

    public static ChatMessageResponse from(ChatMessage message, Long viewerId) {
        boolean own = message.getAuthorId().equals(viewerId);
        return ChatMessageResponse.builder()
                .id(message.getId().toHexString())
                // 다른 작성자의 클라이언트 요청 식별자는 노출할 필요가 없다.
                .clientMessageId(own ? message.getClientMessageId() : null)
                .scope(message.getScope())
                .gameType(message.getGameType())
                .roomId(message.getRoomId())
                .nickname(message.getAuthorNickname())
                .avatarId(message.getAuthorAvatarId())
                .content(message.getContent())
                // MongoDB Date의 밀리초 절삭과 무관하게 방송·이력·상태 응답의 최초 접수 시각을 일치시킨다.
                .createdAt(Instant.ofEpochSecond(message.getAcceptedAtMicros() / 1_000_000,
                        message.getAcceptedAtMicros() % 1_000_000 * 1000))
                .ownMessage(own)
                .build();
    }
}
