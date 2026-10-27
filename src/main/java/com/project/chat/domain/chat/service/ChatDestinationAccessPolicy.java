package com.project.chat.domain.chat.service;

import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.domain.chat.enums.ChatScope;
import com.project.chat.domain.chat.enums.ChatGameType;
import com.project.chat.domain.chat.repository.RedisRoomChatAccessRepository;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

/** 게임 방 참가자 검증에 back의 짧은 참가 리스를 사용하며 비참가자의 저장·조회·실시간 수신을 거부한다. */
@Component @RequiredArgsConstructor
public class ChatDestinationAccessPolicy {
    private final RedisRoomChatAccessRepository rooms;
    private final com.project.chat.domain.social.SocialAccessRepository social;

    /** AUTH 시 대상을 고정하고 로그인 시각과 현재 입장 시각 중 늦은 경계만 공개한다. */
    public Mono<ChatPrincipal> bind(ChatPrincipal principal, ChatDestination destination) {
        if (destination.getScope() == ChatScope.SOCIAL && destination.getConversationId() == null) return Mono.just(principal);
        if (destination.getScope() == ChatScope.GLOBAL) return Mono.just(principal);
        if (destination.getGameType() != ChatGameType.GOMOKU) return denied();
        return rooms.membership(destination.getRoomId(), principal.getUserId()).map(value -> {
            long joined = Long.parseLong(value.substring(0, value.indexOf('|')));
            return principal.toBuilder().roomId(destination.getRoomId()).roomMembership(value)
                    .roomStartedAtMicros(joined).build();
        });
    }

    public Mono<Void> authorize(ChatPrincipal principal, ChatDestination destination) {
        if (destination.getScope() == ChatScope.SOCIAL) return social.require(principal.getUserId(), destination.getConversationId())
                .filter(com.project.chat.domain.social.SocialAccessRepository.Access::isWritable)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.SOCIAL_ACCESS_DENIED))).then();
        if (destination.getScope() == ChatScope.GLOBAL) return principal.getRoomId() == null ? Mono.empty() : denied();
        // 실제 게임방 참가/퇴장 상태를 요청·전파마다 다시 확인하고 클라이언트의 방 번호만 신뢰하지 않는다.
        if (destination.getGameType() != ChatGameType.GOMOKU || !destination.getRoomId().equals(principal.getRoomId())) return denied();
        return rooms.membership(destination.getRoomId(), principal.getUserId())
                .filter(value -> value.equals(principal.getRoomMembership()))
                .switchIfEmpty(denied()).then();
    }

    private <T> Mono<T> denied() { return Mono.error(new BusinessException(ErrorCode.CHAT_ROOM_ACCESS_DENIED)); }
}
