package com.project.chat.domain.presence.dto.response;

import lombok.Builder;
import lombok.Getter;

/** 접속자 공개 목록에는 로그인 세션·계정 ID·브라우저 연결 ID를 포함하지 않는다. */
@Getter
@Builder
public class ChatOnlineUserResponse {
    private final String nickname;
    private final String avatarId;
}
