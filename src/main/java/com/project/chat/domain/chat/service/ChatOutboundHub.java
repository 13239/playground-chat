package com.project.chat.domain.chat.service;

import com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.global.security.ChatPrincipal;
import com.project.chat.auth.service.ChatAuthenticationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.util.function.Function;

/** Kafka broadcast 토픽에서 받은 메시지를 현재 JVM의 인증된 WebSocket 연결에만 전달한다. */
@Component
@RequiredArgsConstructor
public class ChatOutboundHub {

    private static final int CONNECTION_BUFFER = 128;
    // 느린 연결이 다른 연결을 지연시키지 않도록 직접 전달하고 연결별 버퍼 초과 시 해당 연결을 종료한다.
    private final Sinks.Many<ChatBroadcastEvent> broadcasts = Sinks.many().multicast().directBestEffort();
    private final ChatDestinationAccessPolicy accessPolicy;
    private final ChatAuthenticationService authentication;

    public synchronized void publish(ChatBroadcastEvent event) {
        // 서로 다른 Kafka 파티션 소비자가 동시에 발행해도 sink의 비직렬화 실패로 이벤트를 잃지 않는다.
        Sinks.EmitResult result = broadcasts.tryEmitNext(event);
        if (result != Sinks.EmitResult.OK && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER
                && result != Sinks.EmitResult.FAIL_OVERFLOW) {
            throw new IllegalStateException("채팅 로컬 전파에 실패했습니다: " + result);
        }
        // 수신자가 없거나 전원이 느린 경우 저장 이력이 진실의 원천이다. 새 연결은 이력을 재조회한다.
    }

    public Flux<ChatBroadcastEvent> subscribe(ChatPrincipal principal, ChatDestination destination) {
        return withSubscription(principal, destination, Function.identity());
    }

    /** 권한 확인과 실시간 구독을 끝낸 뒤 인증 완료·이력 처리를 시작하며 공백 동안 최대 128개만 보관한다. */
    public <T> Flux<T> withSubscription(ChatPrincipal principal, ChatDestination destination,
                                      Function<Flux<ChatBroadcastEvent>, Flux<T>> output) {
        return accessPolicy.authorize(principal, destination)
                .thenMany(Flux.defer(() -> {
                    var buffer = Sinks.many().unicast().<ChatBroadcastEvent>onBackpressureBuffer(
                            Queues.<ChatBroadcastEvent>get(CONNECTION_BUFFER).get());
                    // 수신 측 demand가 0이어도 포화 오류를 즉시 전달해 버퍼를 비우기를 기다리지 않고 종료한다.
                    Sinks.Empty<Void> failed = Sinks.empty();
                    // 명시적인 구독은 연결 자원으로 관리한다. 구독 설치가 반환된 뒤에만 output을 실행한다.
                    // 느린 연결이 다른 연결을 지연시키지 않도록 직접 전달하고 연결별 버퍼 초과 시 해당 연결을 종료한다.
                    return Flux.using(
                            () -> broadcasts.asFlux()
                                    .filter(event -> event.getConversationKey().equals(destination.getConversationKey()))
                                    // 이력뿐 아니라 지연 도착한 실시간 방송에도 현재 로그인 세션의 공개 경계를 적용한다.
                                    .filter(event -> event.getAcceptedAtMicros() != null
                                            && event.getAcceptedAtMicros() > principal.getMessageBoundaryMicros())
                                    .subscribe(event -> {
                                        Sinks.EmitResult result = buffer.tryEmitNext(event);
                                        // 소비 구독 전 가득 찬 unicast 버퍼는 ZERO_SUBSCRIBER로 반환되어도 같은 초과 오류다.
                                        if (result == Sinks.EmitResult.FAIL_OVERFLOW || result == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                                            failed.tryEmitError(Exceptions.failWithOverflow("채팅 연결 버퍼를 초과했습니다."));
                                        }
                                    }, error -> failed.tryEmitError(error), () -> buffer.tryEmitComplete()),
                            // 이미 버퍼에 들어온 방 방송도 퇴장·재입장 이후에는 전달하지 않는다.
                            ignored -> output.apply(buffer.asFlux().concatMap(event ->
                                    // 제한된 세션에는 주기 종료를 기다리는 동안에도 새 방송을 노출하지 않는다.
                                    authentication.validate(principal).then(accessPolicy.authorize(principal, destination)).thenReturn(event), 1))
                                    .takeUntilOther(failed.asMono()), Disposable::dispose);
                }));
    }
}
