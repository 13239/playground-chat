package com.project.chat.auth.service;

import com.project.chat.auth.ChatConnectionProperties;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** 인증 이전에도 IP별 예산을 Redis에서 공유하며 공격자가 JVM 전체의 인증 대기를 독점하지 못하게 한다. */
@Slf4j
@Component
public class ChatConnectionAdmission {
    private static final RedisScript<Long> ADMIT = RedisScript.of(
            new ClassPathResource("redis/chat/admit-connection.lua"), Long.class);
    private static final RedisScript<Long> RELEASE = RedisScript.of(
            new ClassPathResource("redis/chat/release-connection.lua"), Long.class);
    private final ReactiveStringRedisTemplate redis;
    private final ChatConnectionProperties properties;
    private final Set<String> trustedProxies;

    public ChatConnectionAdmission(ReactiveStringRedisTemplate redis, ChatConnectionProperties properties) {
        this.redis = redis;
        this.properties = properties;
        this.trustedProxies = properties.getTrustedProxies().stream()
                .map(value -> literal(value).getHostAddress()).collect(Collectors.toUnmodifiableSet());
    }

    public Mono<Lease> acquire(ServerWebExchange exchange) {
        return Mono.defer(() -> {
            var remote = exchange.getRequest().getRemoteAddress();
            if (remote == null || remote.getAddress() == null) return Mono.error(invalid());
            InetAddress address = remote.getAddress();
            // 전달 헤더는 실제 TCP 상대가 등록된 프록시일 때만 신뢰한다. 임의 X-Forwarded-For는 사용하지 않는다.
            if (trustedProxies.contains(address.getHostAddress())) {
                List<String> values = exchange.getRequest().getHeaders().get("X-Real-IP");
                if (values == null || values.size() != 1) return Mono.error(invalid());
                address = literal(values.getFirst());
            }
            byte[] bytes = address.getAddress();
            // IPv6 임시 주소를 바꾸며 제한을 우회하지 못하도록 같은 /64에 인증 전 예산을 적용한다.
            if (bytes.length == 16) Arrays.fill(bytes, 8, 16, (byte) 0);
            String hash = hash(HexFormat.of().formatHex(bytes));
            String pendingKey = "chat:admission:{" + hash + "}:pending";
            String id = UUID.randomUUID().toString();
            return redis.execute(ADMIT, List.of("chat:admission:{" + hash + "}:attempts", pendingKey),
                            List.of(id, Integer.toString(properties.getMaxPendingPerIp()), "12000",
                                    Integer.toString(properties.getBurst()), Integer.toString(properties.getAttemptsPerSecond())))
                    .single().timeout(Duration.ofSeconds(3))
                    .onErrorMap(exception -> new BusinessException(ErrorCode.CHAT_SERVICE_UNAVAILABLE))
                    .flatMap(result -> result == 1L ? Mono.just(new Lease(pendingKey, id))
                            : Mono.error(new BusinessException(ErrorCode.CHAT_CONNECTION_RATE_LIMIT_EXCEEDED)));
        });
    }

    private static InetAddress literal(String value) {
        try {
            // DNS 조회와 zone/여러 IP 표기를 허용하지 않고 Java 25의 IP 리터럴 파서를 사용한다.
            if (value.isBlank() || value.length() > 45 || value.contains("%")) throw invalid();
            return InetAddress.ofLiteral(value);
        } catch (IllegalArgumentException exception) {
            throw invalid();
        }
    }

    private static BusinessException invalid() {
        return new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST);
    }

    private static String hash(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.US_ASCII)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256을 사용할 수 없습니다.", exception);
        }
    }

    /** 정상 인증 직후 대기 자리만 반환한다. 이후 연결 수는 사용자별 presence 상한으로 관리한다. */
    public final class Lease {
        private final String key;
        private final String id;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(String key, String id) { this.key = key; this.id = id; }

        public Mono<Void> release() {
            return Mono.defer(() -> {
                if (!released.compareAndSet(false, true)) return Mono.empty();
                return redis.execute(RELEASE, List.of(key), List.of(id)).then()
                        .timeout(Duration.ofSeconds(3)).onErrorResume(exception -> {
                            log.warn("채팅 인증 대기 리스 해제를 완료하지 못했습니다. TTL 만료를 기다립니다.");
                            return Mono.empty();
                        });
            });
        }
    }
}
