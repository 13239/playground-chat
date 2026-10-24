package com.project.chat.domain.adminchat.dto;

import lombok.Builder;
import lombok.Getter;
import java.time.Instant;

/** 관리자 전용 저장 스냅샷이다. 로그인 정보·재전송 식별자·대화 키는 공개하지 않는다. */
@Getter @Builder
public class AdminChatMessage {
    private final String id;
    private final Instant sentAt;
    private final Long authorId;
    private final String authorNickname;
    private final String authorAvatarId;
    private final String scope;
    private final String gameCode;
    private final Long roomId;
    private final String content;
}
