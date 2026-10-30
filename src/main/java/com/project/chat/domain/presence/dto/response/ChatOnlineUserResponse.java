package com.project.chat.domain.presence.dto.response;

import lombok.Builder;
import lombok.Getter;

/** 접속자 공개 목록에는 로그인 세션·내부 계정 ID·브라우저 연결 ID를 포함하지 않는다. */
@Getter
@Builder
public class ChatOnlineUserResponse {
    // 친구 관계 조회는 채팅 작성자와 같은 영속 공개 UUID로만 연결한다.
    private final String publicUserId;
    private final String nickname;
    private final String avatarId;
    // 닉네임이 같아도 인증된 조회자만 본인으로 표시하며 내부 회원 번호는 노출하지 않는다.
    private final boolean self;
}
