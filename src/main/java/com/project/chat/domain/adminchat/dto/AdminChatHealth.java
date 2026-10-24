package com.project.chat.domain.adminchat.dto;

import lombok.Builder;
import lombok.Getter;
import java.time.Instant;

/** 내부 운영 화면에는 종합 상태와 확인 시각만 공개하고 인프라별 오류·주소는 숨긴다. */
@Getter @Builder
public class AdminChatHealth {
    private final String state;
    private final Instant checkedAt;
}
