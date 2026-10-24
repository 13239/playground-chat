package com.project.chat.global.security;

import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.handler.SecurityProblemWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.web.server.*;
import reactor.core.publisher.Mono;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.HexFormat;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

/** DB 권한 검사를 마친 back의 10초 일회용 증명을 요청 경로·쿼리에 묶어 원자 소비한다. */
@RequiredArgsConstructor
public class AdminChatCapabilityFilter implements WebFilter {
    private static final PathPattern INTERNAL = new PathPatternParser().parse("/internal/admin/chat/**");
    private final ReactiveStringRedisTemplate redis;
    private final SecurityProblemWriter problems;
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        var request = exchange.getRequest();
        String path = request.getPath().pathWithinApplication().value();
        // Security와 같은 디코딩 경로로 범위를 잡고 아래에서 정규 원문 경로만 승인한다.
        if (!INTERNAL.matches(request.getPath().pathWithinApplication()) && !path.startsWith("/internal/admin/chat")) return chain.filter(exchange);
        var headers = request.getHeaders().get("X-Chat-Admin-Capability");
        if (request.getMethod() != HttpMethod.GET || !path.matches("/internal/admin/chat/messages(?:/[a-f0-9]{24})?")
                || headers == null || headers.size() != 1 || !headers.getFirst().matches("[A-Za-z0-9_-]{43}")
                || request.getHeaders().containsHeader("Origin")) return problems.write(exchange, ErrorCode.ACCESS_DENIED);
        String target = "GET\n" + request.getURI().getRawPath() + (request.getURI().getRawQuery() == null ? "" : "?" + request.getURI().getRawQuery());
        // 원문 증명은 저장·기록하지 않는다. 다른 경로 사용과 재전송 모두 같은 접근 거부로 끝낸다.
        return redis.opsForValue().getAndDelete("chat:admin:capability:" + digest(headers.getFirst()))
                .map(binding -> MessageDigest.isEqual(binding.getBytes(StandardCharsets.US_ASCII), digest(target).getBytes(StandardCharsets.US_ASCII)))
                .defaultIfEmpty(false)
                .onErrorResume(error -> problems.write(exchange, ErrorCode.CHAT_SERVICE_UNAVAILABLE).then(Mono.empty()))
                .flatMap(valid -> valid ? chain.filter(exchange) : problems.write(exchange, ErrorCode.ACCESS_DENIED));
    }
    private String digest(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException exception) { throw new IllegalStateException(exception); }
    }
}
