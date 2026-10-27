package com.project.chat.domain.chat.dto.internal;

import com.project.chat.domain.chat.enums.ChatGameType;
import com.project.chat.domain.chat.enums.ChatScope;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.UUID;

/** WebSocket에서 인증·권한·내용 검증을 마친 요청만 Kafka 저장 큐에 전달한다. */
@Getter
@Builder
@Jacksonized
public class ChatSendCommand {
    private final int version;
    private final UUID clientMessageId;
    private final ChatScope scope;
    private final ChatGameType gameType;
    private final Long roomId;
    private final String conversationId;
    private final Long authorId;
    private final String authorNickname;
    private final String authorAvatarId;
    private final String content;
    // Kafka 지연·재처리와 무관한 Redis 최초 접수 시각이다. 클라이언트 입력으로 받지 않는다.
    private final Long acceptedAtMicros;
}
