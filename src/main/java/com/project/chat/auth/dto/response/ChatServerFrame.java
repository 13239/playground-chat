package com.project.chat.auth.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

/** 연결 인증 결과와 제어 응답만 제공하며 인증 토큰이나 로그인 세션 ID를 노출하지 않는다. */
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatServerFrame {
    private final int version;
    private final String type;
    private final UUID connectionId;
    private final ChatUserResponse user;
}
