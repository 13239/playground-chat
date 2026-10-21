package com.project.chat.auth.websocket;

import com.project.chat.auth.dto.request.ChatClientFrame;
import com.project.chat.auth.dto.response.ChatServerFrame;
import com.project.chat.auth.dto.response.ChatUserResponse;
import com.project.chat.auth.service.ChatAuthenticationService;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.domain.chat.dto.response.ChatHistoryResponse;
import com.project.chat.domain.chat.enums.ChatScope;
import com.project.chat.domain.chat.kafka.ChatKafkaPublisher;
import com.project.chat.domain.chat.service.ChatMessageService;
import com.project.chat.domain.chat.service.ChatOutboundHub;
import com.project.chat.domain.presence.service.ChatPresenceService;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
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
    private static final int MAX_FRAME_BYTES = 4096;
    private final AtomicInteger pendingAuthentications = new AtomicInteger();

    private final ChatAuthenticationService authentication;
    private final ChatMessageService messages;
    private final ChatKafkaPublisher publisher;
    private final ChatOutboundHub outboundHub;
    private final ChatPresenceService presence;
    private final JsonMapper jsonMapper;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return Mono.defer(() -> {
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
                return authentication.authenticate(decode(first.get()))
                        .doOnNext(principal -> release.run())
                        .flatMapMany(principal -> responses(principal, remaining.skip(1)));
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
        });
    }

    private Flux<ChatServerFrame> responses(ChatPrincipal principal, Flux<String> remaining) {
        UUID connectionId = UUID.randomUUID();
        ChatServerFrame ready = ChatServerFrame.builder().version(1).type("AUTHENTICATED")
                .connectionId(connectionId).user(ChatUserResponse.from(principal)).build();
        AtomicLong lastPing = new AtomicLong();
        AtomicLong lastSend = new AtomicLong();
        AtomicLong lastHistory = new AtomicLong();
        AtomicLong lastPresence = new AtomicLong();
        Sinks.Empty<Void> remoteClosed = Sinks.empty();

        Flux<ChatServerFrame> controls = remaining.concatMap(text -> {
            ChatClientFrame frame = decode(text);
            if (!Integer.valueOf(1).equals(frame.getVersion()) || frame.getTicket() != null
                    || frame.getProof() != null || frame.getType() == null) {
                return Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            }
            // 접속자 페이지 필드는 PRESENCE_GET에서만 허용한다.
            if (!"PRESENCE_GET".equals(frame.getType())
                    && (frame.getOnlineOffset() != null || frame.getOnlineLimit() != null)) {
                return Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            }
            return switch (frame.getType()) {
                case "PING" -> {
                    if (hasChatFields(frame)) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                    }
                    if (!rateLimit(lastPing, Duration.ofSeconds(1))) {
                        yield Flux.error(new BusinessException(ErrorCode.CHAT_CONTROL_RATE_LIMIT_EXCEEDED));
                    }
                    yield authentication.validate(principal)
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
                    if (hasChatFields(frame)) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                    }
                    if (!rateLimit(lastPresence, Duration.ofSeconds(1))) {
                        yield Flux.error(new BusinessException(ErrorCode.CHAT_CONTROL_RATE_LIMIT_EXCEEDED));
                    }
                    yield presence.snapshot(principal, frame.getOnlineOffset(), frame.getOnlineLimit())
                            .map(snapshot -> ChatServerFrame.builder()
                                    .version(1).type("PRESENCE_SNAPSHOT")
                                    .onlineCount(snapshot.getOnlineCount())
                                    .onlineUsers(snapshot.getUsers()).hasMore(snapshot.isHasMore())
                                    .nextOnlineOffset(snapshot.getNextOnlineOffset()).build()).flux();
                }
                case "CHAT_HISTORY" -> {
                    if (frame.getRequestId() == null || frame.getClientMessageId() != null
                            || frame.getContent() != null || !rateLimit(lastHistory, Duration.ofMillis(500))) {
                        yield Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
                    }
                    yield messages.history(principal, destination(frame), frame.getBefore(), frame.getLimit())
                            .flatMapMany(page -> historyFrames(page, frame.getRequestId()));
                }
                default -> Flux.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            };
        }, 1).doOnComplete(() -> remoteClosed.tryEmitEmpty());

        // GAME_ROOM의 참가 권한은 아직 확인되지 않는다. 8단계 정책 연결 전에는 GLOBAL만 실시간으로 구독한다.
        ChatDestination home = ChatDestination.builder().scope(ChatScope.GLOBAL).build();
        Flux<ChatServerFrame> live = outboundHub.subscribe(principal, home)
                .map(event -> ChatServerFrame.builder().version(1).type("CHAT_MESSAGE")
                        .message(event.toViewer(principal.getUserId())).build());

        // 자동 세션 연장 없이 로그아웃·만료·계정 차단·브라우저 키 변경을 최대 약 5초 주기로 반영한다.
        Mono<Void> invalidation = Flux.interval(Duration.ofSeconds(5))
                .concatMap(ignored -> presence.renew(principal, connectionId), 1).then();
        // AUTHENTICATED를 반드시 먼저 전송하며 이후 서버 전파와 클라이언트 요청을 병합한다.
        Flux<ChatServerFrame> presenceUpdates = Flux.concat(
                presence.onlineCount().map(this::presenceChanged),
                // 느린 연결은 오래된 중간 집계치를 건너뛰고 최신 수치만 반영한다.
                presence.changedCounts().onBackpressureLatest().map(this::presenceChanged));
        // 연결 등록 성공 후에만 AUTHENTICATED를 보낸다. 정상 종료·오류·취소가 모두 Redis 등록 해제로 귀결된다.
        // Redis가 이미 끊겨 정리에 실패한 경우에도 연결 리스가 30초 뒤 자연 만료된다.
        return Flux.usingWhen(
                presence.connect(principal, connectionId).thenReturn(connectionId),
                ignored -> Flux.concat(Mono.just(ready), Flux.merge(controls, live, presenceUpdates))
                        // 수신 완료만으로는 live/PubSub이 계속 살아 있으므로 연결 종료를 별도 신호로 전달한다.
                        .takeUntilOther(Mono.firstWithSignal(invalidation, remoteClosed.asMono())),
                ignored -> cleanupPresence(principal, connectionId),
                (ignored, error) -> cleanupPresence(principal, connectionId),
                ignored -> cleanupPresence(principal, connectionId));
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
        return frame.getScope() != null || frame.getGameType() != null || frame.getRoomId() != null
                || frame.getClientMessageId() != null || frame.getContent() != null || frame.getBefore() != null
                || frame.getLimit() != null || frame.getRequestId() != null;
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
