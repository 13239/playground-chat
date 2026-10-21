package com.project.chat.domain.chat.service;

import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;

import java.nio.charset.StandardCharsets;

/** 빈 메시지·제어 문자·과도한 UTF-8 페이로드를 저장 단계에서도 방어한다. */
public final class ChatMessageContentValidator {

    private static final int MAX_CODE_POINTS = 1000;
    private static final int MAX_UTF8_BYTES = 3500;

    private ChatMessageContentValidator() {
    }

    public static String validate(String content) {
        if (content == null) throw new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE);
        // WebSocket 이후 다른 소비 경로가 생겨도 비정상적인 대용량 입력을 먼저 차단한다.
        if (content.length() > MAX_UTF8_BYTES) {
            throw new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE);
        }
        String normalized = content.strip();
        if (normalized.isEmpty() || normalized.codePointCount(0, normalized.length()) > MAX_CODE_POINTS
                || normalized.getBytes(StandardCharsets.UTF_8).length > MAX_UTF8_BYTES) {
            throw new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE);
        }
        for (int index = 0; index < normalized.length();) {
            int cp = normalized.codePointAt(index);
            // 줄바꿈과 탭 외의 제어 문자·서로게이트 오류·방향 제어 문자는 허용하지 않는다.
            if (Character.isISOControl(cp) && cp != '\n' && cp != '\t'
                    || Character.isSurrogate(normalized.charAt(index)) && Character.charCount(cp) == 1
                    || Character.getType(cp) == Character.FORMAT) {
                throw new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE);
            }
            index += Character.charCount(cp);
        }
        return normalized;
    }
}
