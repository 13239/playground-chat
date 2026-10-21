package com.project.chat.auth.dto.response;

import com.project.chat.global.security.ChatPrincipal;
import lombok.Builder;
import lombok.Getter;

/** 공개 프로필은 닉네임·이미지 ID 두 필드로 제한한다. */
@Getter
@Builder
public class ChatUserResponse {
    private final String nickname;
    private final String avatarId;

    public static ChatUserResponse from(ChatPrincipal principal) {
        return ChatUserResponse.builder()
                .nickname(principal.getNickname()).avatarId(principal.getAvatarId()).build();
    }
}
