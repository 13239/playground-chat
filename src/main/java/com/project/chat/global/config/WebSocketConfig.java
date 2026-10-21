package com.project.chat.global.config;

import com.project.chat.auth.websocket.ChatConnectionWebSocketHandler;
import com.project.chat.auth.ChatConnectionProperties;
import com.project.chat.auth.service.ChatConnectionAdmission;
import com.project.chat.domain.chat.kafka.ChatBroadcastReadiness;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.security.handler.SecurityProblemWriter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy;
import reactor.netty.http.server.WebsocketServerSpec;
import reactor.core.publisher.Mono;

import java.util.Map;

/** WebFlux 전용 업그레이드와 프레임 상한을 적용하며 Servlet/STOMP 서버는 생성하지 않는다. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ChatConnectionProperties.class)
public class WebSocketConfig {

    @Bean
    public HandlerMapping chatWebSocketMapping(ChatConnectionWebSocketHandler handler) {
        return new SimpleUrlHandlerMapping(Map.of("/ws/chat", handler), -1);
    }

    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter(ChatConnectionWebSocketHandler handler,
            ChatConnectionAdmission admission, SecurityProblemWriter errors, ChatBroadcastReadiness readiness) {
        var strategy = new ReactorNettyRequestUpgradeStrategy(
                () -> WebsocketServerSpec.builder().maxFramePayloadLength(4096));
        var service = new HandshakeWebSocketService(strategy);
        // IP 제한은 HTTP 업그레이드 전에 적용하고 성공·실패·취소에서 모두 인증 대기 리스를 해제한다.
        // 방송 준비 전에는 업그레이드·Redis IP 예산 확보·티켓 소비 없이 즉시 503으로 거절한다.
        return new WebSocketHandlerAdapter((exchange, mappedHandler) -> readiness.requireReady()
                .then(Mono.defer(() -> Mono.usingWhen(
                        admission.acquire(exchange),
                        lease -> service.handleRequest(exchange, session -> handler.handle(session, lease)),
                        ChatConnectionAdmission.Lease::release,
                        (lease, error) -> lease.release(), ChatConnectionAdmission.Lease::release)))
                .onErrorResume(BusinessException.class, exception -> errors.write(exchange, exception.getErrorCode())));
    }
}
