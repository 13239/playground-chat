package com.project.chat.global.config;

import com.project.chat.auth.websocket.ChatConnectionWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy;
import reactor.netty.http.server.WebsocketServerSpec;

import java.util.Map;

/** WebFlux 전용 업그레이드와 프레임 상한을 적용하며 Servlet/STOMP 서버는 생성하지 않는다. */
@Configuration(proxyBeanMethods = false)
public class WebSocketConfig {

    @Bean
    public HandlerMapping chatWebSocketMapping(ChatConnectionWebSocketHandler handler) {
        return new SimpleUrlHandlerMapping(Map.of("/ws/chat", handler), -1);
    }

    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter() {
        var strategy = new ReactorNettyRequestUpgradeStrategy(
                () -> WebsocketServerSpec.builder().maxFramePayloadLength(4096));
        return new WebSocketHandlerAdapter(new HandshakeWebSocketService(strategy));
    }
}
