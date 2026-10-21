package com.project.chat.domain.chat.service;

import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.domain.chat.enums.ChatScope;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** 게임 방 참가자 검증이 연결되기 전까지는 방 메시지의 저장·조회 모두 명시적으로 거부한다. */
@Component
public class ChatDestinationAccessPolicy {

    public Mono<Void> authorize(ChatPrincipal principal, ChatDestination destination) {
        if (destination.getScope() != ChatScope.GLOBAL) {
            // 8단계에서 실제 게임방 참가/퇴장 상태를 서버에서 검증하는 정책으로 교체한다.
            return Mono.error(new BusinessException(ErrorCode.CHAT_ROOM_ACCESS_DENIED));
        }
        return Mono.empty();
    }
}
