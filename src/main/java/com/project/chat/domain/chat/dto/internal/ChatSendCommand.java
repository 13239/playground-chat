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
    private final Long authorId;
    private final String authorNickname;
    private final String authorAvatarId;
    private final String content;
}
