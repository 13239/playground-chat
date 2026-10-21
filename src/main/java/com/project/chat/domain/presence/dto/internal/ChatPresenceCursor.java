package com.project.chat.domain.presence.dto.internal;

import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

/** 커서는 연결 안에만 보관한다. 브라우저에는 무작위 토큰만 보내 내부 사용자 키를 노출하지 않는다. */
@Getter
@Builder
public class ChatPresenceCursor {
    private final UUID token;
    private final String afterUserId;
}
