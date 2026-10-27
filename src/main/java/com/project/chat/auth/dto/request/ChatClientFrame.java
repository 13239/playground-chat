package com.project.chat.auth.dto.request;

import com.project.chat.domain.chat.enums.ChatGameType;
import com.project.chat.domain.chat.enums.ChatScope;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.UUID;

/** 연결 인증 후 PING·채팅 저장·이력·접속자 목록 요청을 같은 버전의 텍스트 프레임으로 처리한다. */
@Getter
@Builder
@Jacksonized
public class ChatClientFrame {
    private final Integer version;
    private final String type;
    private final String ticket;
    private final String proof;
    // 작성자 ID와 작성 시각은 수신하지 않는다. 대상은 별도 접근 정책으로 검증한다.
    private final ChatScope scope;
    private final ChatGameType gameType;
    private final Long roomId;
    // 소셜 연결은 하나를 유지하며 개인·그룹방의 공개 UUID만 명령마다 검증한다.
    private final String conversationId;
    private final Integer page;
    private final String messageId;
    private final UUID clientMessageId;
    private final String content;
    private final String before;
    private final Integer limit;
    private final UUID requestId;
    // 접속자 목록 팝업은 고정 순서 커서의 페이지 단위로 불러와 큰 JSON과 offset 이동을 방지한다.
    private final UUID onlineCursor;
    private final Integer onlineLimit;
}
