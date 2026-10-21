package com.project.chat.domain.chat.service;

import com.project.chat.domain.chat.entity.ChatMessage;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import org.bson.types.ObjectId;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** 페이지 커서는 조회 조건만 표현하며 채팅방 접근 권한을 대신하지 않는다. */
@Getter
@Builder(access = AccessLevel.PRIVATE)
public class ChatMessageCursor {

    // Redis 접수 시각으로 정렬해 저장 순서와 서비스별 시계 차이의 영향을 받지 않는다.
    private final long acceptedAtMicros;
    private final ObjectId id;

    public static String encode(ChatMessage message) {
        String value = "v2:" + message.getAcceptedAtMicros() + ":" + message.getId().toHexString();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.US_ASCII));
    }

    public static ChatMessageCursor decode(String cursor) {
        if (cursor == null || cursor.isBlank() || cursor.length() > 128
                || !cursor.matches("[A-Za-z0-9_-]+")) {
            throw new BusinessException(ErrorCode.INVALID_CHAT_CURSOR);
        }
        try {
            String value = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.US_ASCII);
            String[] tokens = value.split(":", -1);
            if (tokens.length != 3 || !"v2".equals(tokens[0])
                    || !tokens[1].matches("[0-9]{1,18}")
                    || !ObjectId.isValid(tokens[2]) || tokens[2].length() != 24) {
                throw new IllegalArgumentException("Invalid cursor");
            }
            long time = Long.parseLong(tokens[1]);
            if (time <= 0) throw new IllegalArgumentException("Invalid cursor timestamp");
            return ChatMessageCursor.builder()
                    .acceptedAtMicros(time)
                    .id(new ObjectId(tokens[2]))
                    .build();
        } catch (IllegalArgumentException exception) {
            throw new BusinessException(ErrorCode.INVALID_CHAT_CURSOR);
        }
    }
}
