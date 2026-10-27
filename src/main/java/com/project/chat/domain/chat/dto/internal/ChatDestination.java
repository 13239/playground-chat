package com.project.chat.domain.chat.dto.internal;

import com.project.chat.domain.chat.enums.ChatGameType;
import com.project.chat.domain.chat.enums.ChatScope;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import lombok.Builder;
import lombok.Getter;

import java.util.Locale;

/** 검증된 채팅 대상을 MongoDB·Kafka·Redis가 공유할 하나의 대화 식별자로 변환한다. */
@Getter
public class ChatDestination {

    private final ChatScope scope;
    private final ChatGameType gameType;
    private final Long roomId;
    private final String conversationId;

    @Builder
    private ChatDestination(ChatScope scope, ChatGameType gameType, Long roomId, String conversationId) {
        // builder나 이후 역직렬화 경로에서도 공용 채팅에 방 번호를 섞거나 잘못된 방을 만들지 못한다.
        if (scope == null || scope != ChatScope.SOCIAL && conversationId != null
                || scope == ChatScope.SOCIAL && (gameType != null || roomId != null)
                || scope == ChatScope.GLOBAL && (gameType != null || roomId != null)
                || scope == ChatScope.GAME_ROOM && (gameType == null || roomId == null || roomId <= 0)) {
            throw new BusinessException(ErrorCode.INVALID_CHAT_DESTINATION);
        }
        this.scope = scope;
        this.gameType = gameType;
        this.roomId = roomId;
        if (conversationId != null) {
            try {
                if (!java.util.UUID.fromString(conversationId).toString().equals(conversationId)) throw new IllegalArgumentException();
            } catch (RuntimeException exception) { throw new BusinessException(ErrorCode.INVALID_CHAT_DESTINATION); }
        }
        this.conversationId = conversationId;
    }

    public String getConversationKey() {
        // 이 값은 라우팅 식별자이며 참가 권한을 증명하지 않는다. 실제 입장 권한은 서버에서 별도 확인한다.
        return switch (scope) {
            case GLOBAL -> "global";
            case SOCIAL -> conversationId == null ? "social" : "social:" + conversationId;
            case GAME_ROOM -> "game:" + gameType.name().toLowerCase(Locale.ROOT) + ":room:" + roomId;
        };
    }
}
