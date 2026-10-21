package com.project.chat.domain.chat.support;

import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;

import java.time.Instant;

/** 저장·방송·조회 응답의 시각을 Redis 최초 접수 마이크로초에서 동일하게 복원한다. */
public final class ChatMessageTime {

    private ChatMessageTime() {
    }

    public static Instant fromAcceptedAtMicros(Long acceptedAtMicros) {
        return fromAcceptedAtMicros(acceptedAtMicros, null);
    }

    /** 정확한 불변 시각은 재사용해 방송을 받는 사용자 수만큼 같은 Instant를 새로 만들지 않는다. */
    public static Instant fromAcceptedAtMicros(Long acceptedAtMicros, Instant existingTime) {
        // 공개 경계를 알 수 없는 구버전 메시지를 현재 시각이나 DB 표시 시각으로 대체하지 않는다.
        if (acceptedAtMicros == null || acceptedAtMicros <= 0) {
            throw new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE);
        }
        long seconds = acceptedAtMicros / 1_000_000;
        long nanos = acceptedAtMicros % 1_000_000 * 1000;
        if (existingTime != null && existingTime.getEpochSecond() == seconds && existingTime.getNano() == nanos) {
            return existingTime;
        }
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
