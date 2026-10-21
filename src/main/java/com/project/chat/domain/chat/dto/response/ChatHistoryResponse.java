package com.project.chat.domain.chat.dto.response;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/** 서버가 보장하는 시간 오름차순 목록과 이전 페이지 탐색용 불투명 커서를 반환한다. */
@Getter
@Builder
public class ChatHistoryResponse {

    private final List<ChatMessageResponse> messages;
    private final boolean hasMore;
    private final String nextCursor;
}
