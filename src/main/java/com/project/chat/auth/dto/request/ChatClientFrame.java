package com.project.chat.auth.dto.request;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

/** 2단계에서는 AUTH와 PING만 받으며 작성자나 메시지 전송 필드는 수신하지 않는다. */
@Getter
@Builder
@Jacksonized
public class ChatClientFrame {
    private final Integer version;
    private final String type;
    private final String ticket;
    private final String proof;
}
