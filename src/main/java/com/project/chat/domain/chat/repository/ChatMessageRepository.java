package com.project.chat.domain.chat.repository;

import com.project.chat.domain.chat.entity.ChatMessage;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

/** 저장 요청의 중복 재시도 시 원본 메시지만 조회한다. 영속성 중복 차단은 MongoDB 고유 인덱스가 담당한다. */
public interface ChatMessageRepository extends ReactiveMongoRepository<ChatMessage, ObjectId> {

    Mono<ChatMessage> findByConversationKeyAndAuthorIdAndClientMessageId(
            String conversationKey, Long authorId, String clientMessageId);
}
