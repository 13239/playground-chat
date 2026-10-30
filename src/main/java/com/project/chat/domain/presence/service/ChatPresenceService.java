package com.project.chat.domain.presence.service;

import com.project.chat.auth.service.ChatAuthenticationService;
import com.project.chat.domain.presence.dto.response.ChatPresenceSnapshot;
import com.project.chat.domain.presence.repository.RedisChatPresenceRepository;
import com.project.chat.global.security.ChatPrincipal;
import com.project.chat.global.security.ChatRequestRateLimiter;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.UUID;

/** 인증 연결의 존재를 Redis 리스로 관리하고, 모든 chat 서버에 실시간 접속자 변경을 전달한다. */
@Service
public class ChatPresenceService {

    private final RedisChatPresenceRepository repository;
    private final ChatAuthenticationService authentication;
    private final ReactiveRedisMessageListenerContainer listener;
    private final Flux<Long> changes;
    private final ChatRequestRateLimiter rateLimiter;

    public ChatPresenceService(RedisChatPresenceRepository repository,
                               ChatAuthenticationService authentication,
                               ReactiveRedisMessageListenerContainer listener, ChatRequestRateLimiter rateLimiter) {
        this.repository = repository;
        this.authentication = authentication;
        this.listener = listener;
        this.rateLimiter = rateLimiter;
        // Redis Pub/Sub은 유실될 수 있으므로 20초 주기 재동기화로 숫자를 보정한다.
        // 공유 구독은 chat 서버마다 하나만 두어 WebSocket 접속 수만큼 Redis 구독이 생성되지 않게 한다.
        this.changes = Flux.merge(
                        listener.receive(ChannelTopic.of(RedisChatPresenceRepository.CHANGES_CHANNEL)).map(ignored -> 0L),
                        Flux.interval(Duration.ofSeconds(20)))
                .concatMap(ignored -> repository.onlineCount(), 1)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(20)))
                // 신규 연결에 이전 접속자 수가 재생되지 않도록 최신 변경만 공유한다.
                .publish().refCount(1);
    }

    public Mono<Void> connect(ChatPrincipal principal, UUID connectionId) {
        // 티켓은 WebSocket에서 이미 소비했지만 Redis 상태를 새로 노출하기 직전에 세션을 재확인한다.
        return authentication.validate(principal).then(repository.connect(principal, connectionId));
    }

    public Mono<Void> renew(ChatPrincipal principal, UUID connectionId) {
        // 세션이 만료되면 갱신에 실패하고 상위 WebSocket 구독 취소 경로에서 등록을 해제한다.
        return authentication.validate(principal).then(repository.renew(principal.getUserId(), connectionId));
    }

    public Mono<Void> disconnect(ChatPrincipal principal, UUID connectionId) {
        return repository.disconnect(principal.getUserId(), connectionId);
    }

    public Mono<ChatPresenceSnapshot> snapshot(ChatPrincipal principal, String afterUserId, Integer requestedSize) {
        int size = requestedSize == null ? 50 : requestedSize;
        if (size < 1 || size > 100) {
            return Mono.error(new BusinessException(ErrorCode.INVALID_PRESENCE_REQUEST));
        }
        return authentication.validate(principal).then(rateLimiter.read(principal.getUserId()))
                // 본인 표시는 클라이언트 입력이 아닌 검증된 연결의 회원 번호로만 결정한다.
                .then(repository.snapshot(afterUserId, size, principal.getUserId()));
    }

    public Mono<Long> onlineCount() {
        return repository.onlineCount();
    }

    /** 접속자 변동·비정상 종료·Redis 알림 누락 시에도 화면의 온라인 수를 갱신한다. */
    public Flux<Long> changedCounts() {
        return changes;
    }
}
