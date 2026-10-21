package com.project.chat.domain.chat.repository;

import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/** back이 DB 커밋 후 갱신한 짧은 참가 리스만 읽는다. chat은 참가 권한을 직접 만들거나 연장하지 않는다. */
@Repository @RequiredArgsConstructor
public class RedisRoomChatAccessRepository {
    private final ReactiveStringRedisTemplate redis;

    public Mono<String> membership(Long roomId, Long userId) {
        return redis.<String, String>opsForHash().get("gomoku:room:access:" + roomId, "user:" + userId)
                .filter(value -> value.matches("[1-9][0-9]{0,17}\\|[a-f0-9-]{36}"))
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.CHAT_ROOM_ACCESS_DENIED)))
                .onErrorMap(DataAccessException.class, error -> new BusinessException(ErrorCode.CHAT_SERVICE_UNAVAILABLE));
    }
}
