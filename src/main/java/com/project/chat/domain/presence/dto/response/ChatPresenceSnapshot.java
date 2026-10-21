package com.project.chat.domain.presence.dto.response;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/** 현재 접속자는 탭·기기가 아닌 서로 다른 사용자 수를 기준으로 집계한다. */
@Getter
@Builder
public class ChatPresenceSnapshot {
    private final long onlineCount;
    private final List<ChatOnlineUserResponse> users;
    private final boolean hasMore;
    private final Integer nextOnlineOffset;
}
