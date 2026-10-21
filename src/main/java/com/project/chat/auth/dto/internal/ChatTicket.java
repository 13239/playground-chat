package com.project.chat.auth.dto.internal;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

/** back이 Redis에 저장한 인증 대상이며 브라우저 요청으로 직접 생성하지 않는다. */
@Getter
@Builder
public class ChatTicket {
    private final Long userId;
    private final UUID sessionId;
    private final String keyThumbprint;
    private final String nickname;
    private final String avatarId;
    private final String origin;
    private final String websocketUrl;
    private final Instant issuedAt;
    private final Instant expiresAt;
    // 발급 시각이 아니라 최초 로그인에서 고정한 홈 채팅 공개 경계다.
    private final long chatStartedAtMicros;
}
