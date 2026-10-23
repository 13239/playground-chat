package com.project.chat.auth.repository;

import com.project.chat.auth.dto.internal.ChatTicket;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** back과 같은 Redis DB에서 로그인 세션을 확인하되 토큰·수명을 수정하지 않는다. */
@Repository
@RequiredArgsConstructor
public class RedisChatAuthRepository {

    private static final RedisScript<Long> CONSUME = AccessPolicyScripts.script("redis/chat/consume-ticket.lua", 3);
    private static final RedisScript<Long> VALIDATE = AccessPolicyScripts.script("redis/chat/validate-session.lua", 1);

    private final ReactiveStringRedisTemplate redis;

    public Mono<ChatTicket> find(String hash) {
        return redis.<String, String>opsForHash().entries("chat:auth:ticket:" + hash)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .map(this::decode)
                .onErrorMap(DataAccessException.class, exception -> unavailable());
    }

    public Mono<ChatPrincipal> consume(String hash, ChatTicket ticket) {
        String sid = ticket.getSessionId().toString();
        String uid = ticket.getUserId().toString();
        return redis.execute(CONSUME, List.of(
                "chat:auth:ticket:" + hash, "chat:auth:pending:" + sid,
                "auth:session:" + sid, "auth:user:sessions:" + uid, "auth:account:blocked:" + uid
        ), List.of(sid, uid, ticket.getKeyThumbprint(), ticket.getOrigin(), ticket.getWebsocketUrl(),
                Long.toString(ticket.getChatStartedAtMicros())))
                .single()
                .flatMap(result -> {
                    // 접속 제한은 4401로 연결을 종료하고 BFF의 세션 재확인에서 관리자의 안내 문구를 가져온다.
                    if (result == -2L) return Mono.error(new BusinessException(ErrorCode.ACCESS_RESTRICTED));
                    if (result == -3L) return Mono.error(new BusinessException(ErrorCode.ACCESS_POLICY_UNAVAILABLE));
                    if (result == -1L) return Mono.error(new BusinessException(ErrorCode.INVALID_SESSION));
                    if (result != 1L) return Mono.error(new BusinessException(ErrorCode.CHAT_TICKET_INVALID));
                    return Mono.just(ChatPrincipal.builder()
                            .userId(ticket.getUserId()).sessionId(ticket.getSessionId())
                            .keyThumbprint(ticket.getKeyThumbprint())
                            .nickname(ticket.getNickname()).avatarId(ticket.getAvatarId())
                            .chatStartedAtMicros(ticket.getChatStartedAtMicros()).build());
                })
                .onErrorMap(DataAccessException.class, exception -> unavailable());
    }

    public Mono<Void> validate(ChatPrincipal principal) {
        String sid = principal.getSessionId().toString();
        String uid = principal.getUserId().toString();
        return redis.execute(VALIDATE, List.of(
                "auth:session:" + sid, "auth:user:sessions:" + uid, "auth:account:blocked:" + uid
        ), List.of(sid, uid, principal.getKeyThumbprint(), Long.toString(principal.getChatStartedAtMicros())))
                .single()
                .flatMap(result -> result == 1L ? Mono.<Void>empty()
                        : Mono.error(new BusinessException(result == -2L ? ErrorCode.ACCESS_RESTRICTED
                        : result == -3L ? ErrorCode.ACCESS_POLICY_UNAVAILABLE : ErrorCode.INVALID_SESSION)))
                .onErrorMap(DataAccessException.class, exception -> unavailable());
    }

    private ChatTicket decode(Map<String, String> fields) {
        try {
            Long uid = Long.valueOf(fields.get("userId"));
            UUID sid = UUID.fromString(fields.get("sessionId"));
            String nickname = fields.get("nickname");
            String avatar = fields.get("avatarId");
            String key = fields.get("keyThumbprint");
            long started = Long.parseLong(fields.get("chatStartedAtMicros"));
            if (uid <= 0 || nickname == null || nickname.isBlank() || nickname.codePointCount(0, nickname.length()) > 50
                    || avatar == null || !avatar.matches("[A-Z_]{2,20}")
                    || key == null || !(key.matches("[A-Za-z0-9_-]{43}") || key.equals("dev-http"))
                    || started <= 0 || fields.get("origin") == null || fields.get("websocketUrl") == null) {
                throw new IllegalArgumentException();
            }
            return ChatTicket.builder().userId(uid).sessionId(sid).keyThumbprint(key)
                    .nickname(nickname).avatarId(avatar)
                    .origin(fields.get("origin")).websocketUrl(fields.get("websocketUrl"))
                    .issuedAt(Instant.ofEpochMilli(Long.parseLong(fields.get("issuedAt"))))
                    .expiresAt(Instant.ofEpochMilli(Long.parseLong(fields.get("expiresAt"))))
                    .chatStartedAtMicros(started).build();
        } catch (IllegalArgumentException | NullPointerException exception) {
            // 손상·만료·소비된 증명을 모두 같은 공개 오류로 처리한다.
            throw new BusinessException(ErrorCode.CHAT_TICKET_INVALID);
        }
    }

    private BusinessException unavailable() {
        return new BusinessException(ErrorCode.CHAT_SERVICE_UNAVAILABLE);
    }
}
