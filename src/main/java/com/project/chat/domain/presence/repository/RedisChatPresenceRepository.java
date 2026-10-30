package com.project.chat.domain.presence.repository;

import com.project.chat.domain.chat.support.ChatAvatarCatalog;
import com.project.chat.domain.chat.service.ChatProfiles;

import com.project.chat.domain.presence.dto.response.ChatOnlineUserResponse;
import com.project.chat.domain.presence.dto.response.ChatPresenceSnapshot;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

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
    private static final String ORDERED_USERS = "chat:presence:{online}:ordered";
    public static final String CHANGES_CHANNEL = "chat:presence:{online}:changed";
    private static final String CONNECTIONS_PREFIX = "chat:presence:{online}:connections:";
    private static final String PROFILE_PREFIX = "chat:presence:{online}:profile:";
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final int MAX_CONNECTIONS_PER_USER = 16;
    private static final RedisScript<Long> CONNECT = script("redis/presence/connect.lua");
    private static final RedisScript<Long> RENEW = script("redis/presence/renew.lua");
    private static final RedisScript<Long> DISCONNECT = script("redis/presence/disconnect.lua");
    private static final RedisScript<Long> COUNT = script("redis/presence/count.lua");
    private static final RedisScript<List> PAGE = RedisScript.of(new ClassPathResource("redis/presence/page.lua"), List.class);

    private final ReactiveStringRedisTemplate redis;
    private final ChatProfiles profiles;

    public Mono<Void> connect(ChatPrincipal principal, UUID connectionId) {
        return execute(CONNECT, principal.getUserId(),
                List.of(principal.getUserId().toString(), connectionId.toString(),
                        Long.toString(LEASE.toMillis()),
                        principal.getNickname(), principal.getAvatarId(),
                        Integer.toString(MAX_CONNECTIONS_PER_USER)))
                .flatMap(result -> result == 1L ? Mono.<Void>empty()
                        : Mono.error(new BusinessException(ErrorCode.CHAT_CONNECTION_LIMIT_EXCEEDED)));
    }

    public Mono<Void> renew(long userId, UUID connectionId) {
        return execute(RENEW, userId,
                List.of(Long.toString(userId), connectionId.toString(),
                        Long.toString(LEASE.toMillis())))
                // 만료된 연결을 다시 활성화하지 않는다. WebSocket을 종료하고 새 티켓으로 재인증한다.
                .flatMap(result -> result == 1L ? Mono.<Void>empty() : Mono.error(unavailable()));
    }

    public Mono<Void> disconnect(long userId, UUID connectionId) {
        return execute(DISCONNECT, userId,
                List.of(Long.toString(userId), connectionId.toString()))
                .then();
    }

    /** 만료 시각보다 미래인 사용자만 집계한다. stale 항목은 다음 Lua 갱신에서 정리한다. */
    public Mono<Long> onlineCount() {
        return redis.execute(COUNT, List.of(USERS)).single()
                .onErrorMap(exception -> unavailable());
    }

    /** 접속자 목록은 요청 시에만 조회하며, 각 사용자에 연결이 여러 개여도 프로필을 한 번만 반환한다. */
    public Mono<ChatPresenceSnapshot> snapshot(String afterUserId, int size, long viewerId) {
        // 전체 개수와 페이지 경계를 원자적으로 읽으며 offset 증가에 따른 탐색 비용을 없앤다.
        return redis.execute(PAGE, List.of(USERS, ORDERED_USERS),
                        List.of(afterUserId == null ? "" : afterUserId, Integer.toString(size), Long.toString(viewerId))).single()
                // Redis의 List.class 반환 경계에서만 와일드카드로 바꾸고 이후에는 raw 타입을 전파하지 않는다.
                .<List<?>>map(page -> page)
                .flatMap(page -> Flux.fromIterable(page.subList(2, page.size()))
                        .map(String::valueOf)
                        .flatMapSequential(userId -> publicProfile(userId, viewerId), 16)
                        .collectList()
                        .map(users -> ChatPresenceSnapshot.builder()
                                .onlineCount(Long.parseLong(String.valueOf(page.getFirst())))
                                // 첫 페이지의 본인은 닉네임 정렬보다 우선하고 나머지는 기존 순서를 유지한다.
                                .users(users.stream().sorted(Comparator.comparing(ChatOnlineUserResponse::isSelf).reversed()
                                        .thenComparing(ChatOnlineUserResponse::getNickname))
                                        .toList())
                                .hasMore(!String.valueOf(page.get(1)).isEmpty())
                                .nextUserId(String.valueOf(page.get(1)).isEmpty() ? null : String.valueOf(page.get(1)))
                                .build()))
                .onErrorMap(exception -> unavailable());
    }

    private Mono<ChatOnlineUserResponse> publicProfile(String userId, long viewerId) {
        // 프로필의 TTL은 리스보다 길다. 만료 경합으로 없어진 프로필은 노출하지 않는다.
        return redis.<String, String>opsForHash().entries(PROFILE_PREFIX + userId)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .filter(values -> values.containsKey("nickname") && values.containsKey("avatarId"))
                // 재접속 없이도 인증 서버가 관리하는 최신 공개 UUID·닉네임·아바타를 함께 조회한다.
                .flatMap(values -> profiles.resolve(Long.valueOf(userId), values.get("nickname"), values.get("avatarId")))
                .map(profile -> ChatOnlineUserResponse.builder()
                        .publicUserId(profile.getPublicUserId())
                        .self(Long.parseLong(userId) == viewerId)
                        // 기존 접속자의 Redis 리스가 남아 있어도 새 카탈로그 밖의 이미지는 노출하지 않는다.
                        .nickname(profile.getNickname()).avatarId(ChatAvatarCatalog.normalize(profile.getAvatarId())).build());
    }

    private Mono<Long> execute(RedisScript<Long> script, long userId, List<String> args) {
        return redis.execute(script, List.of(USERS, CONNECTIONS_PREFIX + userId,
                        PROFILE_PREFIX + userId, CHANGES_CHANNEL, ORDERED_USERS), args)
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
