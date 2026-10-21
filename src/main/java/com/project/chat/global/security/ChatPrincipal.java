package com.project.chat.global.security;

import lombok.Builder;
import lombok.Getter;

import java.security.Principal;
import java.util.UUID;

/** 원자적인 일회용 증명 소비와 로그인 세션 검증을 통과한 WebSocket 연결의 내부 사용자다. */
@Getter
@Builder(toBuilder = true)
public class ChatPrincipal implements Principal {

    private final Long userId;
    private final UUID sessionId;
    private final String keyThumbprint;
    private final String nickname;
    private final String avatarId;
    // 재연결·토큰 갱신에도 유지하며 클라이언트가 지정한 시각은 사용하지 않는다.
    private final long chatStartedAtMicros;

    // 방 연결은 입장 당시의 참가 식별자에 묶는다. 퇴장 뒤 재입장해도 이전 소켓은 권한을 되찾지 못한다.
    private final Long roomId;
    private final String roomMembership;
    private final long roomStartedAtMicros;

    /** 로그인 검증의 원래 경계는 보존하고 메시지 공개에만 입장 경계를 추가한다. */
    public long getMessageBoundaryMicros() { return Math.max(chatStartedAtMicros, roomStartedAtMicros); }

    @Override
    public String getName() {
        // 로그인 아이디는 읽지 않으며 이 내부 식별자를 공개 응답에 직렬화하지 않는다.
        return userId.toString();
    }
}
