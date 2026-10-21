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
    // 내부 사용자 키는 WebSocket에서 연결 전용 무작위 커서로 바꿔 공개한다.
    private final String nextUserId;
}
