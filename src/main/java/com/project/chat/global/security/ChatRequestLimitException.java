package com.project.chat.global.security;

import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import lombok.Getter;

/** 정상 연결을 끊지 않고 해당 요청만 거절하며 클라이언트에 제한된 재시도 시간을 알린다. */
@Getter
public class ChatRequestLimitException extends BusinessException {
    private final long retryAfterMillis;

    public ChatRequestLimitException(long retryAfterMillis) {
        super(ErrorCode.CHAT_REQUEST_RATE_LIMIT_EXCEEDED);
        this.retryAfterMillis = retryAfterMillis;
    }
}
