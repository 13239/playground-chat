package com.project.chat.auth.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.project.chat.domain.chat.dto.response.ChatMessageResponse;
import com.project.chat.domain.chat.dto.response.ChatAuthorProfile;
import com.project.chat.domain.presence.dto.response.ChatOnlineUserResponse;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

/** 인증·송신 접수·저장 완료 방송·커서 이력·접속자 변경을 구분해 클라이언트에 전달한다. */
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatServerFrame {
    private final int version;
    private final String type;
    private final UUID connectionId;
    private final ChatUserResponse user;
    private final UUID clientMessageId;
    private final UUID requestId;
    private final ChatMessageResponse message;
    // 이미 본 작성자의 프로필 변경만 전달해 과거 메시지의 표시 이름도 함께 갱신한다.
    private final ChatAuthorProfile profile;
    private final Boolean hasMore;
    private final String nextCursor;
    // 접속자 정보에는 개인 세션이나 내부 계정 ID를 포함하지 않는다.
    private final Long onlineCount;
    private final List<ChatOnlineUserResponse> onlineUsers;
    private final UUID nextOnlineCursor;
    // 미확정 전송은 본인의 UUID로 저장 여부를 확인하며 일시적인 요청 제한은 연결을 종료하지 않는다.
    private final Boolean stored;
    private final String operation;
    private final String code;
    private final Long retryAfterMillis;
}
