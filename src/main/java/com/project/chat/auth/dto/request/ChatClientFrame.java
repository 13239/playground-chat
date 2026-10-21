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
    private final UUID clientMessageId;
    private final String content;
    private final String before;
    private final Integer limit;
    private final UUID requestId;
    // 접속자 목록 팝업은 페이지 단위로 불러와 서버가 큰 JSON 프레임을 생성하지 않게 한다.
    private final Integer onlineOffset;
    private final Integer onlineLimit;
}
