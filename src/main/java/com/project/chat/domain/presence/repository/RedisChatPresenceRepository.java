package com.project.chat.domain.presence.repository;

import com.project.chat.domain.presence.dto.response.ChatOnlineUserResponse;
import com.project.chat.domain.presence.dto.response.ChatPresenceSnapshot;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Redis Lua로 사용자·연결 인덱스를 함께 갱신해 다중 서버에서도 온라인 수를 중복 집계하지 않는다. */
@Repository
@RequiredArgsConstructor
public class RedisChatPresenceRepository {

    // 모든 관련 키를 같은 Redis Cluster 슬롯에 배치해 원자적 Lua 갱신을 보장한다.
    private static final String USERS = "chat:presence:{online}:users";
    public static final String CHANGES_CHANNEL = "chat:presence:{online}:changed";
    private static final String CONNECTIONS_PREFIX = "chat:presence:{online}:connections:";
    private static final String PROFILE_PREFIX = "chat:presence:{online}:profile:";
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final int MAX_CONNECTIONS_PER_USER = 16;
    private static final RedisScript<Long> CONNECT = script("redis/presence/connect.lua");
    private static final RedisScript<Long> RENEW = script("redis/presence/renew.lua");
    private static final RedisScript<Long> DISCONNECT = script("redis/presence/disconnect.lua");

    private final ReactiveStringRedisTemplate redis;

    public Mono<Void> connect(ChatPrincipal principal, UUID connectionId) {
        return execute(CONNECT, principal.getUserId(),
                List.of(principal.getUserId().toString(), connectionId.toString(),
                        Long.toString(System.currentTimeMillis()), Long.toString(LEASE.toMillis()),
                        principal.getNickname(), principal.getAvatarId(),
                        Integer.toString(MAX_CONNECTIONS_PER_USER)))
                .flatMap(result -> result == 1L ? Mono.<Void>empty()
                        : Mono.error(new BusinessException(ErrorCode.CHAT_CONNECTION_LIMIT_EXCEEDED)));
    }

    public Mono<Void> renew(long userId, UUID connectionId) {
        return execute(RENEW, userId,
                List.of(Long.toString(userId), connectionId.toString(),
                        Long.toString(System.currentTimeMillis()), Long.toString(LEASE.toMillis())))
                // 만료된 연결을 다시 활성화하지 않는다. WebSocket을 종료하고 새 티켓으로 재인증한다.
                .flatMap(result -> result == 1L ? Mono.<Void>empty() : Mono.error(unavailable()));
    }

    public Mono<Void> disconnect(long userId, UUID connectionId) {
        return execute(DISCONNECT, userId,
                List.of(Long.toString(userId), connectionId.toString(),
                        Long.toString(System.currentTimeMillis())))
                .then();
    }

    /** 만료 시각보다 미래인 사용자만 집계한다. stale 항목은 다음 Lua 갱신에서 정리한다. */
    public Mono<Long> onlineCount() {
        return redis.opsForZSet().count(USERS,
                Range.open((double) System.currentTimeMillis(), Double.POSITIVE_INFINITY))
                .onErrorMap(exception -> unavailable());
    }

    /** 접속자 목록은 요청 시에만 조회하며, 각 사용자에 연결이 여러 개여도 프로필을 한 번만 반환한다. */
    public Mono<ChatPresenceSnapshot> snapshot(int offset, int size) {
        long now = System.currentTimeMillis();
        return redis.opsForZSet().count(USERS, Range.open((double) now, Double.POSITIVE_INFINITY))
                .flatMap(count -> redis.opsForZSet()
                        .rangeByScore(USERS, Range.open((double) now, Double.POSITIVE_INFINITY),
                                Limit.limit().offset(offset).count(size))
                        .flatMapSequential(this::publicProfile, 16)
                        .collectList()
                        .map(users -> ChatPresenceSnapshot.builder()
                                .onlineCount(count)
                                .users(users.stream().sorted(Comparator.comparing(ChatOnlineUserResponse::getNickname))
                                        .toList())
                                .hasMore(count > (long) offset + size)
                                .nextOnlineOffset(count > (long) offset + size ? offset + size : null)
                                .build()))
                .onErrorMap(exception -> unavailable());
    }

    private Mono<ChatOnlineUserResponse> publicProfile(String userId) {
        // 프로필의 TTL은 리스보다 길다. 만료 경합으로 없어진 프로필은 노출하지 않는다.
        return redis.<String, String>opsForHash().entries(PROFILE_PREFIX + userId)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .filter(values -> values.containsKey("nickname") && values.containsKey("avatarId"))
                .map(values -> ChatOnlineUserResponse.builder()
                        .nickname(values.get("nickname")).avatarId(values.get("avatarId")).build());
    }

    private Mono<Long> execute(RedisScript<Long> script, long userId, List<String> args) {
        return redis.execute(script, List.of(USERS, CONNECTIONS_PREFIX + userId,
                        PROFILE_PREFIX + userId, CHANGES_CHANNEL), args)
                .single()
                .onErrorMap(exception -> unavailable());
    }

    private static RedisScript<Long> script(String location) {
        return RedisScript.of(new ClassPathResource(location), Long.class);
    }

    private BusinessException unavailable() {
        return new BusinessException(ErrorCode.CHAT_SERVICE_UNAVAILABLE);
    }
}
