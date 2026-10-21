package com.project.chat.domain.chat.repository;

import com.project.chat.domain.chat.entity.ChatMessage;
import lombok.RequiredArgsConstructor;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.time.Instant;

/** offset/skip 없이 (작성 시각, MongoDB _id) 복합 커서로 이전 메시지를 찾는다. */
@Repository
@RequiredArgsConstructor
public class ChatMessageHistoryRepository {

    private static final Sort NEWEST_FIRST = Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("_id"));
    private final ReactiveMongoTemplate mongoTemplate;

    public Flux<ChatMessage> findRecent(String conversationKey, Instant beforeTime, ObjectId beforeId, int fetchSize) {
        Criteria conversation = Criteria.where("conversationKey").is(conversationKey);
        if (beforeTime != null && beforeId != null) {
            // 같은 밀리초의 메시지도 _id로 안정적으로 정렬하므로 페이지 경계에서 중복·누락되지 않는다.
            Criteria older = new Criteria().orOperator(
                    Criteria.where("createdAt").lt(beforeTime),
                    new Criteria().andOperator(
                            Criteria.where("createdAt").is(beforeTime), Criteria.where("_id").lt(beforeId)));
            conversation = new Criteria().andOperator(conversation, older);
        }
        return mongoTemplate.find(Query.query(conversation).with(NEWEST_FIRST).limit(fetchSize), ChatMessage.class);
    }
}
