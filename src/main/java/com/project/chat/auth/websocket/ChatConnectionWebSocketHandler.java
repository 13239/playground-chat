package com.project.chat.auth.websocket;

import com.project.chat.auth.dto.request.ChatClientFrame;
import com.project.chat.auth.dto.response.ChatServerFrame;
import com.project.chat.auth.dto.response.ChatUserResponse;
import com.project.chat.auth.service.ChatAuthenticationService;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.Disposable;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** 2단계의 인증·제어 연결이다. 게임 구독과 채팅 메시지는 이후 송수신 단계에서 추가한다. */
@Component
@RequiredArgsConstructor
public class ChatConnectionWebSocketHandler implements WebSocketHandler {

    private static final int MAX_PENDING_AUTHENTICATIONS = 128;
    private static final int MAX_FRAME_BYTES = 4096;
    private final AtomicInteger pendingAuthentications = new AtomicInteger();

    private final ChatAuthenticationService authentication;
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

        Flux<ChatServerFrame> controls = remaining.concatMap(text -> {
            ChatClientFrame frame = decode(text);
            long now = System.nanoTime();
            long previous = lastPing.getAndSet(now);
            if (!Integer.valueOf(1).equals(frame.getVersion()) || !"PING".equals(frame.getType())
                    || frame.getTicket() != null || frame.getProof() != null) {
                return Mono.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            }
            if (previous != 0 && now - previous < Duration.ofSeconds(1).toNanos()) {
                return Mono.error(new BusinessException(ErrorCode.CHAT_CONTROL_RATE_LIMIT_EXCEEDED));
            }
            return authentication.validate(principal)
                    .thenReturn(ChatServerFrame.builder().version(1).type("PONG").build());
        }, 1);

        // 자동 세션 연장 없이 로그아웃·만료·계정 차단·브라우저 키 변경을 최대 약 5초 주기로 반영한다.
        Mono<Void> invalidation = Flux.interval(Duration.ofSeconds(5))
                .concatMap(ignored -> authentication.validate(principal), 1).then();
        return Flux.concat(Mono.just(ready), controls).takeUntilOther(invalidation);
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
