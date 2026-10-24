package com.project.chat.domain.adminchat.dto;

import lombok.Builder;
import lombok.Getter;
import java.util.List;

/** 목록과 집계를 같은 집계 명령으로 읽어 번호 페이지의 경계를 맞춘다. */
@Getter @Builder
public class AdminChatPage {
    private final List<AdminChatMessage> items;
    private final int page;
    private final int size;
    private final int totalPages;
    private final long totalElements;
}
