package com.project.chat.global.config;

import com.project.chat.domain.chat.entity.ChatMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.time.Duration;

/** 채팅 이력 탐색과 재시도 중복 차단에 필요한 인덱스를 서비스 기동 전에 준비한다. */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class ChatMessageIndexConfig {

    @Bean
    public SmartInitializingSingleton chatMessageIndexInitializer(ReactiveMongoTemplate mongoTemplate) {
        return () -> {
            // 기존 컬렉션과 메시지는 삭제하지 않으며 필요한 인덱스만 멱등 생성한다.
            // 중복된 기존 데이터가 있다면 unique 인덱스 실패를 숨기지 않고 기동을 중단한다.
            var indexes = mongoTemplate.indexOps(ChatMessage.class);
            var history = new Index()
                    .on("conversationKey", Sort.Direction.ASC)
                    .on("acceptedAtMicros", Sort.Direction.DESC)
                    .on("_id", Sort.Direction.DESC)
                    .named("chat_history_v2");
            var idempotency = new Index()
                    .on("conversationKey", Sort.Direction.ASC)
                    .on("authorId", Sort.Direction.ASC)
                    .on("clientMessageId", Sort.Direction.ASC)
                    .unique()
                    .named("chat_idempotency_v1");
            // 삭제 예정 ensureIndex 대신 현재 API를 사용하며 기존 v1 인덱스·문서는 삭제하지 않는다.
            indexes.createIndex(history)
                    .then(indexes.createIndex(idempotency))
                    // 시작 시점에만 블로킹하며 WebFlux 요청 처리 스레드에서는 블로킹하지 않는다.
                    .block(Duration.ofSeconds(20));
            log.info("채팅 메시지 조회·멱등성 인덱스 준비 완료");
        };
    }
}
