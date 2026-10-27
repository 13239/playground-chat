package com.project.chat.domain.social;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;
import java.time.Duration;
import java.util.*;
import com.project.chat.global.exception.*;
import com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent;

/** back이 커밋한 방 권한만 읽는다. 캐시 부재·Redis 장애를 허용 상태로 대체하지 않는다. */
@Repository @RequiredArgsConstructor
public class SocialAccessRepository {
    private final ReactiveStringRedisTemplate redis;
    private final JsonMapper mapper;
    @Getter @Builder @Jacksonized
    public static class Access {
        private final long since;
        private final long until;
        private final boolean writable;
        private final String membershipId;
        private final List<Long> blocked;
        private final boolean muted;
        private final String kind;
        public boolean visible(Long author,long at) { return at>since&&at<=until&&(blocked==null||!blocked.contains(author)); }
        public boolean visible(ChatBroadcastEvent event) { return event.getAcceptedAtMicros()!=null&&visible(event.getAuthorId(),event.getAcceptedAtMicros()); }
    }
    public static String id(String value) {
        try { if(!UUID.fromString(value).toString().equals(value))throw new IllegalArgumentException();return value; }
        catch(RuntimeException error){throw new BusinessException(ErrorCode.INVALID_CHAT_DESTINATION);}
    }
    public Mono<Access> find(long user,String room) {
        return Mono.defer(()->redis.<String,String>opsForHash().get("social:room:"+id(room),Long.toString(user)))
            .map(value->mapper.readValue(value,Access.class)).timeout(Duration.ofSeconds(3));
    }
    public Mono<Access> require(long user,String room) {
        return find(user,room).switchIfEmpty(Mono.error(new BusinessException(ErrorCode.SOCIAL_ACCESS_DENIED)));
    }
    public Flux<String> rooms(long user) { return redis.opsForSet().members("social:rooms:"+user).take(100); }
    /** 참가자의 내부 키는 읽음 상태 계산 안에서만 사용하며 응답에 노출하지 않는다. */
    public Flux<reactor.util.function.Tuple2<Long,Access>> members(String room) {
        return redis.<String,String>opsForHash().entries("social:room:"+id(room)).take(10)
            .map(entry->reactor.util.function.Tuples.of(Long.parseLong(entry.getKey()),mapper.readValue(entry.getValue(),Access.class)));
    }
    public Mono<Long> now() {
        return redis.execute(RedisScript.of("local t=redis.call('TIME');return t[1]*1000000+t[2]",Long.class),List.of(),List.of()).single();
    }
    public Mono<Void> online(String publicId) {
        return publicId==null?Mono.empty():redis.opsForValue().set("social:online:"+publicId,"1",Duration.ofSeconds(30)).then();
    }
}

