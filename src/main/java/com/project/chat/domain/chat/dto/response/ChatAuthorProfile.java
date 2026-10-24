package com.project.chat.domain.chat.dto.response;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

/** 공개 작성자 키는 무작위 UUID이며 계정 ID·로그인 아이디·세션 정보를 포함하지 않는다. */
@Getter
@Builder
@Jacksonized
public class ChatAuthorProfile {
    private final String authorKey;
    private final String nickname;
    private final String avatarId;
    private final long revision;
}
