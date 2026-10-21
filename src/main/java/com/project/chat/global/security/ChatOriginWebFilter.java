package com.project.chat.global.security;

import com.project.chat.auth.ChatAuthProperties;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.handler.SecurityProblemWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Mono;

/** 업그레이드 전에 Origin을 확인하며 URL·Authorization·subprotocol로 인증 토큰을 받지 않는다. */
@RequiredArgsConstructor
public class ChatOriginWebFilter implements WebFilter {

    private static final PathPattern CHAT_PATH = new PathPatternParser().parse("/ws/chat");

    private final ChatAuthProperties properties;
    private final SecurityProblemWriter problemWriter;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        var request = exchange.getRequest();
        // 보안·라우팅과 같은 PathPattern으로 판별해 인코딩·matrix 경로로 Origin 검사가 빠지지 않게 한다.
        if (!CHAT_PATH.matches(request.getPath().pathWithinApplication())) return chain.filter(exchange);
        var origins = request.getHeaders().get(HttpHeaders.ORIGIN);
        if (origins == null || origins.size() != 1 || !properties.getOrigin().equals(origins.getFirst())) {
            return problemWriter.write(exchange, ErrorCode.CHAT_ORIGIN_INVALID);
        }
        if (!"/ws/chat".equals(request.getPath().pathWithinApplication().value())
                || request.getURI().getRawQuery() != null || request.getHeaders().get(HttpHeaders.AUTHORIZATION) != null
                || request.getHeaders().get("Sec-WebSocket-Protocol") != null) {
            return problemWriter.write(exchange, ErrorCode.INVALID_AUTHENTICATION_REQUEST);
        }
        return chain.filter(exchange);
    }
}
