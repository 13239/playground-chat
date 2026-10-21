package com.project.chat.global.security;

import lombok.Builder;
import lombok.Getter;

import java.security.Principal;
import java.util.UUID;

/** 원자적인 일회용 증명 소비와 로그인 세션 검증을 통과한 WebSocket 연결의 내부 사용자다. */
@Getter
@Builder
public class ChatPrincipal implements Principal {

    private final Long userId;
    private final UUID sessionId;
    private final String keyThumbprint;
    private final String nickname;
    private final String avatarId;

    @Override
    public String getName() {
        // 로그인 아이디는 읽지 않으며 이 내부 식별자를 공개 응답에 직렬화하지 않는다.
        return userId.toString();
    }
}
