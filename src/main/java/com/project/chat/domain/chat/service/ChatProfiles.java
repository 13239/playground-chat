package com.project.chat.domain.chat.service;

import com.project.chat.domain.chat.dto.response.ChatAuthorProfile;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/** 최신 프로필은 인증 Redis에서 읽고 변경 알림은 인스턴스마다 하나의 구독으로 공유한다. */
@Component
public class ChatProfiles {
    private static final String PREFIX = "chat:profiles:{profiles}:user:";
    private final ReactiveStringRedisTemplate redis;
    private final Flux<ChatAuthorProfile> changes;

    public ChatProfiles(ReactiveStringRedisTemplate redis, ReactiveRedisMessageListenerContainer listener, JsonMapper mapper) {
        this.redis = redis;
        this.changes = listener.receive(ChannelTopic.of("chat:profiles:changed:v1"))
                .<ChatAuthorProfile>handle((message, sink) -> {
                    try {
                        var profile = mapper.readValue(message.getMessage(), ChatAuthorProfile.class);
                        if (valid(profile)) sink.next(profile);
                    } catch (RuntimeException ignored) { /* 허용한 공개 프로필 외의 알림은 전파하지 않는다. */ }
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(10)))
                .publish().refCount(1);
    }

    public Flux<ChatAuthorProfile> changes() { return changes; }

    public Mono<ChatAuthorProfile> resolve(Long userId, String nickname, String avatarId) {
        String key = PREFIX + userId;
        return redis.<String, String>opsForHash().entries(key).collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .flatMap(values -> {
                    // 배포 이전 메시지도 최초 조회 때 공개 키만 준비하고 저장 당시 이름은 안전한 대체값으로 사용한다.
                    Mono<String> authorKey = values.containsKey("authorKey") ? Mono.just(values.get("authorKey"))
                            : redis.<String, String>opsForHash().putIfAbsent(key, "authorKey", UUID.randomUUID().toString())
                                    .then(redis.<String, String>opsForHash().get(key, "authorKey"));
                    return authorKey.map(value -> ChatAuthorProfile.builder().authorKey(value)
                            .nickname(values.getOrDefault("nickname", nickname)).avatarId(values.getOrDefault("avatarId", avatarId))
                            .revision(Long.parseLong(values.getOrDefault("revision", "0"))).build());
                });
    }

    private static boolean valid(ChatAuthorProfile profile) {
        if (profile == null || profile.getAuthorKey() == null || profile.getNickname() == null || profile.getNickname().isBlank()
                || profile.getNickname().codePointCount(0, profile.getNickname().length()) > 50
                || profile.getAvatarId() == null || !profile.getAvatarId().matches("[A-Z_]{1,20}") || profile.getRevision() < 1) return false;
        try { return UUID.fromString(profile.getAuthorKey()).toString().equals(profile.getAuthorKey()); }
        catch (IllegalArgumentException ignored) { return false; }
    }
}
