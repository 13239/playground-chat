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

/** offset/skip 없이 (서버 접수 시각, MongoDB _id) 복합 커서로 로그인 이후의 이전 메시지를 찾는다. */
@Repository
@RequiredArgsConstructor
public class ChatMessageHistoryRepository {

    private static final Sort NEWEST_FIRST = Sort.by(Sort.Order.desc("acceptedAtMicros"), Sort.Order.desc("_id"));
    private final ReactiveMongoTemplate mongoTemplate;

    public Flux<ChatMessage> findRecent(String conversationKey, long visibleAfter,
                                       Long beforeTime, ObjectId beforeId, int fetchSize) {
        // 필수 하한은 클라이언트 커서와 별개다. 경계 필드가 없는 기존 문서도 자동으로 제외된다.
        Criteria conversation = new Criteria().andOperator(Criteria.where("conversationKey").is(conversationKey),
                Criteria.where("acceptedAtMicros").gt(visibleAfter));
        if (beforeTime != null && beforeId != null) {
            // 같은 마이크로초의 메시지도 _id로 안정적으로 정렬하므로 페이지 경계에서 중복·누락되지 않는다.
            Criteria older = new Criteria().orOperator(
                    Criteria.where("acceptedAtMicros").lt(beforeTime),
                    new Criteria().andOperator(
                            Criteria.where("acceptedAtMicros").is(beforeTime), Criteria.where("_id").lt(beforeId)));
            conversation = new Criteria().andOperator(conversation, older);
        }
        return mongoTemplate.find(Query.query(conversation).with(NEWEST_FIRST).limit(fetchSize), ChatMessage.class);
    }
}
