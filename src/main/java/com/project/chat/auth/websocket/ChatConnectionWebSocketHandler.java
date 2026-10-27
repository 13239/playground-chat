package com.project.chat.auth.websocket;

import com.project.chat.auth.dto.request.ChatClientFrame;
import com.project.chat.auth.dto.response.ChatServerFrame;
import com.project.chat.auth.dto.response.ChatUserResponse;
import com.project.chat.auth.service.ChatAuthenticationService;
import com.project.chat.auth.service.ChatAccessPolicyChanges;
import com.project.chat.auth.service.ChatConnectionAdmission;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.domain.chat.dto.response.ChatHistoryResponse;
import com.project.chat.domain.chat.enums.ChatScope;
import com.project.chat.domain.chat.kafka.ChatKafkaPublisher;
import com.project.chat.domain.chat.kafka.ChatBroadcastReadiness;
import com.project.chat.domain.chat.service.ChatMessageService;
import com.project.chat.domain.chat.service.ChatOutboundHub;
import com.project.chat.domain.chat.service.ChatDestinationAccessPolicy;
import com.project.chat.domain.chat.service.ChatProfiles;
import com.project.chat.domain.presence.service.ChatPresenceService;
import com.project.chat.domain.presence.dto.internal.ChatPresenceCursor;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import com.project.chat.global.security.ChatRequestLimitException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.UUID;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** 인증·세션 무효화·MongoDB 이력·Kafka 전파에 Redis 접속자 수명주기를 연결한다. */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChatConnectionWebSocketHandler implements WebSocketHandler {

    private static final int MAX_PENDING_AUTHENTICATIONS = 128;
    // 최대 1,000자 이모지 본문과 친구 대화의 UUID 메타데이터를 함께 수용한다.
    private static final int MAX_FRAME_BYTES = 8192;
    private final AtomicInteger pendingAuthentications = new AtomicInteger();

    private final ChatAuthenticationService authentication;
    private final ChatMessageService messages;
    private final ChatKafkaPublisher publisher;
    private final ChatOutboundHub outboundHub;
    private final ChatPresenceService presence;
    private final JsonMapper jsonMapper;
    private final ChatBroadcastReadiness broadcastReadiness;
    private final ChatDestinationAccessPolicy accessPolicy;
    private final ChatAccessPolicyChanges policyChanges;
    private final ChatProfiles profiles;
    private final com.project.chat.domain.social.SocialConnection social;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return handle(session, null);
    }

    /** HTTP 업그레이드 전에 확보한 IP 대기 리스를 인증 직후 반환한다. */
    public Mono<Void> handle(WebSocketSession session, ChatConnectionAdmission.Lease lease) {
        // 업그레이드 검사 이후 방송 준비가 해제된 경우에도 인증·티켓 소비를 시작하지 않는다.
        return broadcastReadiness.requireReady().then(Mono.defer(() -> {
            if (pendingAuthentications.incrementAndGet() > MAX_PENDING_AUTHENTICATIONS) {
                pendingAuthentications.decrementAndGet();
                return session.close(new CloseStatus(1013, "CHAT_SERVICE_UNAVAILABLE"));
            }
            AtomicBoolean pending = new AtomicBoolean(true);
            Runnable release = () -> {
                if (pending.compareAndSet(true, false)) pendingAuthentications.decrementAndGet();
            };
            AtomicReference<Disposable> inbound = new AtomicReference<>();
            // 버퍼는 즉시 복사하며 수신 구독은 종료 프레임 전송이 끝날 때까지 유지한다.
            // 하위 AUTH·timeout 취소가 Netty의 빈 종료 프레임을 먼저 보내지 않도록 분리하고 대기는 한 프레임으로 제한한다.
            Flux<String> frames = session.receive().map(message -> {
                        try { return Signal.next(text(message)); }
                        catch (BusinessException exception) { return Signal.<String>error(exception); }
                    })
                    .publish(1).autoConnect(1, inbound::set)
                    .<String>handle((captured, sink) -> {
                        if (captured.hasValue()) sink.next(captured.get());
                        else sink.error(captured.getThrowable());
                    })
                    .timeout(Mono.delay(Duration.ofSeconds(5)), ignored -> Mono.delay(Duration.ofSeconds(90)));
            Flux<ChatServerFrame> output = frames.switchOnFirst((first, remaining) -> {
                if (first.hasError()) return Flux.error(first.getThrowable());
                if (!first.hasValue()) return Flux.empty();
                ChatClientFrame auth = decode(first.get());
                // URL이나 토큰에 방 번호를 붙이지 않고 첫 AUTH에서 한 연결의 채팅 대상을 고정한다.
                ChatDestination target = ChatDestination.builder().scope(auth.getScope() == null ? ChatScope.GLOBAL : auth.getScope())
                        .gameType(auth.getGameType()).roomId(auth.getRoomId()).build();
                if (auth.getClientMessageId() != null || auth.getContent() != null || auth.getBefore() != null || auth.getLimit() != null
                        || auth.getConversationId() != null || auth.getPage() != null || auth.getMessageId() != null
                        || auth.getOnlineCursor() != null || auth.getOnlineLimit() != null
                        || auth.getRequestId() != null) return Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                return authentication.authenticate(auth)
                        .flatMap(principal -> accessPolicy.bind(principal, target))
                        .doOnNext(principal -> release.run())
                        .delayUntil(principal -> lease == null ? Mono.empty() : lease.release())
                        .flatMapMany(principal -> responses(principal, target, remaining.skip(1)));
            });
            // 오류를 Netty 송신 publisher 밖으로 전파하면 상태 없는 종료가 먼저 나갈 수 있어 종료 프레임을 먼저 보낸다.
            // WebSocket 송신 흐름 하나에서 제어·이력·실시간 이벤트를 직렬화하고 종료 코드를 전달한다.
            Flux<WebSocketMessage> outbound = output
                    .map(frame -> session.textMessage(jsonMapper.writeValueAsString(frame)))
                    .onErrorResume(exception -> session.close(closeStatus(exception)).thenMany(Flux.empty()));
            return session.send(outbound)
                    .onErrorResume(exception -> session.close(closeStatus(exception)))
                    .doFinally(ignored -> {
                        Disposable connection = inbound.get();
                        if (connection != null) connection.dispose();
                        release.run();
                    });
        })).onErrorResume(exception -> session.close(closeStatus(exception)));
    }

    private Flux<ChatServerFrame> responses(ChatPrincipal principal, ChatDestination target, Flux<String> remaining) {
        if (target.getScope() == ChatScope.SOCIAL) return social.responses(principal, remaining);
        // 이 연결에서 이미 공개한 작성자만 알림 대상으로 삼고 메시지 창보다 조금 큰 유한 LRU를 유지한다.
        Map<String, Boolean> knownAuthors = Collections.synchronizedMap(new LinkedHashMap<>(512, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) { return size() > 512; }
        });
        UUID connectionId = UUID.randomUUID();
        ChatServerFrame ready = ChatServerFrame.builder().version(1).type("AUTHENTICATED")
                .connectionId(connectionId).user(ChatUserResponse.from(principal)).build();
        AtomicLong lastPing = new AtomicLong();
        AtomicLong lastSend = new AtomicLong();
        AtomicLong lastHistory = new AtomicLong();
        AtomicLong lastPresence = new AtomicLong();
        AtomicLong lastStatus = new AtomicLong();
        AtomicReference<ChatPresenceCursor> presenceCursor = new AtomicReference<>();
        Sinks.Empty<Void> remoteClosed = Sinks.empty();

        Flux<ChatServerFrame> controls = remaining.concatMap(text -> {
            ChatClientFrame frame = decode(text);
            if (!Integer.valueOf(1).equals(frame.getVersion()) || frame.getTicket() != null
                    || frame.getProof() != null || frame.getType() == null) {
                return Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            }
            // 소셜 필드는 본인 전용 SOCIAL 연결에서만 허용한다.
            if (frame.getConversationId() != null || frame.getPage() != null || frame.getMessageId() != null) {
                return Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            }
            // 접속자 페이지 필드는 PRESENCE_GET에서만 허용한다.
            if (!"PRESENCE_GET".equals(frame.getType())
                    && (frame.getOnlineCursor() != null || frame.getOnlineLimit() != null)) {
                return Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            }
            // 인증한 방과 다른 대상을 보내거나 방 연결에서 공용 접속자 목록을 요청할 수 없다.
            if (("CHAT_SEND".equals(frame.getType()) || "CHAT_HISTORY".equals(frame.getType()) || "CHAT_STATUS".equals(frame.getType()))
                    && !destination(frame).getConversationKey().equals(target.getConversationKey())
                    || "PRESENCE_GET".equals(frame.getType()) && target.getScope() != ChatScope.GLOBAL) {
                return Flux.error(new BusinessException(ErrorCode.CHAT_ROOM_ACCESS_DENIED));
            }
            Flux<ChatServerFrame> response = switch (frame.getType()) {
                case "PING" -> {
                    if (hasChatFields(frame)) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                    }
                    if (!rateLimit(lastPing, Duration.ofSeconds(1))) {
                        yield Flux.error(new BusinessException(ErrorCode.CHAT_CONTROL_RATE_LIMIT_EXCEEDED));
                    }
                    yield authentication.validate(principal)
                            .then(accessPolicy.authorize(principal, target))
                            .thenReturn(ChatServerFrame.builder().version(1).type("PONG").build()).flux();
                }
                case "CHAT_SEND" -> {
                    if (frame.getClientMessageId() == null || frame.getContent() == null
                            || frame.getBefore() != null || frame.getLimit() != null || frame.getRequestId() != null) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE));
                    }
                    if (!rateLimit(lastSend, Duration.ofMillis(200))) {
                        yield Flux.error(new BusinessException(ErrorCode.CHAT_CONTROL_RATE_LIMIT_EXCEEDED));
                    }
                    yield messages.prepare(principal, destination(frame), frame.getClientMessageId(), frame.getContent())
                            .flatMap(publisher::enqueue)
                            // 접수 응답은 저장 성공을 뜻하지 않는다. 저장 완료는 CHAT_MESSAGE 이벤트로만 알린다.
                            .thenReturn(ChatServerFrame.builder().version(1).type("CHAT_ACCEPTED")
                                    .clientMessageId(frame.getClientMessageId()).build()).flux();
                }
                case "PRESENCE_GET" -> {
                    // 목록은 인증된 접속에서만 요청하고 반복 조회 폭주를 제한한다.
                    if (hasMessageFields(frame) || frame.getRequestId() == null) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                    }
                    if (!rateLimit(lastPresence, Duration.ofSeconds(1))) {
                        yield Flux.error(new BusinessException(ErrorCode.CHAT_CONTROL_RATE_LIMIT_EXCEEDED));
                    }
                    // 클라이언트 커서는 이 연결의 마지막 페이지에만 유효하다. 사용자 키를 직접 받지 않는다.
                    ChatPresenceCursor previous = presenceCursor.get();
                    if (frame.getOnlineCursor() != null
                            && (previous == null || !frame.getOnlineCursor().equals(previous.getToken()))) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_PRESENCE_REQUEST));
                    }
                    String after = frame.getOnlineCursor() == null ? null : previous.getAfterUserId();
                    yield presence.snapshot(principal, after, frame.getOnlineLimit())
                            .map(snapshot -> {
                                UUID token = snapshot.isHasMore() ? UUID.randomUUID() : null;
                                presenceCursor.set(token == null ? null : ChatPresenceCursor.builder()
                                        .token(token).afterUserId(snapshot.getNextUserId()).build());
                                return ChatServerFrame.builder()
                                    .version(1).type("PRESENCE_SNAPSHOT").requestId(frame.getRequestId())
                                    .onlineCount(snapshot.getOnlineCount())
                                    .onlineUsers(snapshot.getUsers()).hasMore(snapshot.isHasMore())
                                    .nextOnlineCursor(token).build();
                            }).flux();
                }
                case "CHAT_HISTORY" -> {
                    if (frame.getRequestId() == null || frame.getClientMessageId() != null
                            || frame.getContent() != null || !rateLimit(lastHistory, Duration.ofMillis(500))) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                    }
                    yield messages.history(principal, destination(frame), frame.getBefore(), frame.getLimit())
                            .flatMapMany(page -> historyFrames(page, frame.getRequestId()));
                }
                case "CHAT_STATUS" -> {
                    if (frame.getClientMessageId() == null || frame.getContent() != null || frame.getBefore() != null
                            || frame.getLimit() != null || frame.getRequestId() != null
                            || !rateLimit(lastStatus, Duration.ofMillis(500))) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                    }
                    // 본인의 UUID를 고유 인덱스로 조회하므로 최근 페이지 바깥의 저장 결과도 복구한다.
                    yield messages.status(principal, destination(frame), frame.getClientMessageId())
                            .map(message -> ChatServerFrame.builder().version(1).type("CHAT_STATUS_RESULT")
                                    .clientMessageId(frame.getClientMessageId()).stored(true).message(message).build())
                            .defaultIfEmpty(ChatServerFrame.builder().version(1).type("CHAT_STATUS_RESULT")
                                    .clientMessageId(frame.getClientMessageId()).stored(false).build()).flux();
                }
                default -> Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            };
            // 사용자 전체 예산 초과는 요청만 거절한다. 잘못된 프레임·인증 실패는 기존 종료 정책을 유지한다.
            return response.onErrorResume(ChatRequestLimitException.class, exception -> Mono.just(
                    ChatServerFrame.builder().version(1).type("CHAT_ERROR").operation(frame.getType())
                            .clientMessageId(frame.getClientMessageId()).requestId(frame.getRequestId())
                            .code(exception.getErrorCode().name()).retryAfterMillis(exception.getRetryAfterMillis()).build()));
        }, 1).doOnComplete(() -> remoteClosed.tryEmitEmpty());

        // GAME_ROOM도 AUTH에서 검증한 한 방에만 구독한다. 공용 접속자 수와 방 참가자 수는 섞지 않는다.
        boolean global = target.getScope() == ChatScope.GLOBAL;

        // 자동 세션 연장 없이 로그아웃·만료·계정 차단·브라우저 키 변경을 최대 약 5초 주기로 반영한다.
        // 정책 변경 알림은 즉시 검사하고 Pub/Sub 유실 시에는 기존 주기 검사가 연결을 종료한다.
        Mono<Void> invalidation = Flux.merge(Flux.interval(Duration.ofSeconds(global ? 5 : 2)), policyChanges.changes())
                .concatMap(ignored -> global ? presence.renew(principal, connectionId)
                        : authentication.validate(principal).then(accessPolicy.authorize(principal, target)), 1).then();
        // AUTHENTICATED를 반드시 먼저 전송하며 이후 서버 전파와 클라이언트 요청을 병합한다.
        Flux<ChatServerFrame> presenceUpdates = !global ? Flux.empty() : Flux.concat(
                presence.onlineCount().map(this::presenceChanged),
                // 느린 연결은 오래된 중간 집계치를 건너뛰고 최신 수치만 반영한다.
                presence.changedCounts().onBackpressureLatest().map(this::presenceChanged));
        // 연결 등록 성공 후에만 AUTHENTICATED를 보낸다. 정상 종료·오류·취소가 모두 Redis 등록 해제로 귀결된다.
        // Redis가 이미 끊겨 정리에 실패한 경우에도 연결 리스가 30초 뒤 자연 만료된다.
        // AUTHENTICATED 송신·즉시 도착한 이력 요청보다 실시간 구독을 먼저 설치한다.
        // 이력 조회 중 발생한 방송은 연결 버퍼를 거쳐 전달하고 프론트에서 메시지 ID로 이력과 병합한다.
        return outboundHub.withSubscription(principal, target, events -> {
            // Kafka에 남은 이전 닉네임도 전송 직전에 최신 프로필로 치환한다. Netty 이벤트 루프를 막지 않는다.
            Flux<ChatServerFrame> live = events.concatMap(event -> profiles.resolve(event.getAuthorId(), event.getNickname(), event.getAvatarId())
                    .map(profile -> ChatServerFrame.builder().version(1).type("CHAT_MESSAGE")
                            .message(event.toViewer(principal.getUserId()).withProfile(profile)).build()), 1);
            Flux<ChatServerFrame> profileUpdates = profiles.changes()
                    .filter(profile -> knownAuthors.containsKey(profile.getAuthorKey()))
                    .onBackpressureBuffer(128)
                    .concatMap(profile -> authentication.validate(principal).then(accessPolicy.authorize(principal, target))
                            .thenReturn(ChatServerFrame.builder().version(1).type("CHAT_PROFILE_CHANGED").profile(profile).build()), 1);
            return Flux.usingWhen(
                    (global ? presence.connect(principal, connectionId) : Mono.<Void>empty()).thenReturn(connectionId),
                    ignored -> Flux.concat(Mono.just(ready), Flux.merge(controls, live, presenceUpdates, profileUpdates))
                            .doOnNext(frame -> {
                                if (frame.getMessage() != null && frame.getMessage().getAuthorKey() != null) {
                                    knownAuthors.put(frame.getMessage().getAuthorKey(), true);
                                }
                            })
                            // 수신 완료만으로는 live/PubSub이 계속 살아 있으므로 연결 종료를 별도 신호로 전달한다.
                            .takeUntilOther(Mono.firstWithSignal(invalidation, remoteClosed.asMono())),
                    ignored -> global ? cleanupPresence(principal, connectionId) : Mono.empty(),
                    (ignored, error) -> global ? cleanupPresence(principal, connectionId) : Mono.empty(),
                    ignored -> global ? cleanupPresence(principal, connectionId) : Mono.empty())
                    // 구독 설치 직후부터 방송 재할당·중단을 감시하며 복구 후에는 새 연결의 이력으로 보충한다.
                    .takeUntilOther(broadcastReadiness.unavailableSignal());
        });
    }

    private ChatServerFrame presenceChanged(Long count) {
        return ChatServerFrame.builder().version(1).type("PRESENCE_CHANGED").onlineCount(count).build();
    }

    private Mono<Void> cleanupPresence(ChatPrincipal principal, UUID connectionId) {
        return presence.disconnect(principal, connectionId)
                .onErrorResume(exception -> {
                    // 연결별 Redis 리스는 DB 장애·프로세스 강제 종료에도 자동 만료된다.
                    log.warn("채팅 접속 등록 해제를 즉시 완료하지 못했습니다. TTL 만료를 기다립니다.");
                    return Mono.empty();
                });
    }

    private Flux<ChatServerFrame> historyFrames(ChatHistoryResponse page, UUID requestId) {
        // 큰 페이지를 한 프레임으로 만들지 않고 1개 메시지 = 1개 프레임으로 스트리밍한다.
        return Flux.concat(
                Flux.fromIterable(page.getMessages()).map(message -> ChatServerFrame.builder()
                        .version(1).type("CHAT_HISTORY_ITEM").requestId(requestId).message(message).build()),
                Mono.just(ChatServerFrame.builder().version(1).type("CHAT_HISTORY_END")
                        .requestId(requestId).hasMore(page.isHasMore()).nextCursor(page.getNextCursor()).build()));
    }

    private ChatDestination destination(ChatClientFrame frame) {
        if (frame.getScope() == null) {
            throw new BusinessException(ErrorCode.INVALID_CHAT_DESTINATION);
        }
        return ChatDestination.builder().scope(frame.getScope())
                .gameType(frame.getGameType()).roomId(frame.getRoomId()).build();
    }

    private boolean hasChatFields(ChatClientFrame frame) {
        return hasMessageFields(frame) || frame.getRequestId() != null;
    }

    private boolean hasMessageFields(ChatClientFrame frame) {
        return frame.getScope() != null || frame.getGameType() != null || frame.getRoomId() != null
                || frame.getClientMessageId() != null || frame.getContent() != null || frame.getBefore() != null
                || frame.getLimit() != null;
    }

    private boolean rateLimit(AtomicLong last, Duration minInterval) {
        long now = System.nanoTime();
        long previous = last.getAndSet(now);
        return previous == 0 || now - previous >= minInterval.toNanos();
    }

    private String text(WebSocketMessage message) {
        if (message.getType() != WebSocketMessage.Type.TEXT
                || message.getPayload().readableByteCount() > MAX_FRAME_BYTES) {
            throw new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST);
        }
        return message.getPayloadAsText();
    }

    private ChatClientFrame decode(String text) {
        try {
            ChatClientFrame frame = jsonMapper.readValue(text, ChatClientFrame.class);
            if (frame == null) throw new IllegalArgumentException();
            return frame;
        } catch (Exception exception) {
            throw new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST);
        }
    }

    private CloseStatus closeStatus(Throwable exception) {
        if (exception instanceof TimeoutException) return new CloseStatus(4408, "CHAT_AUTHENTICATION_TIMEOUT");
        if (Exceptions.isOverflow(exception)) return new CloseStatus(1013, "CHAT_SLOW_CONSUMER");
        if (exception instanceof BusinessException business) {
            ErrorCode code = business.getErrorCode();
            int status = switch (code.getStatus().value()) {
                case 401 -> 4401;
                case 403 -> 4403;
                case 429 -> 1008;
                case 503 -> 1013;
                default -> 4400;
            };
            return new CloseStatus(status, code.name());
        }
        return new CloseStatus(1011, "CHAT_SERVICE_UNAVAILABLE");
    }
}
