package com.project.chat.domain.adminchat;

import com.project.chat.domain.adminchat.dto.*;
import com.project.chat.domain.chat.support.ChatMessageTime;
import com.project.chat.global.exception.*;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

/** 일반 채팅의 로그인 이후 커서 조회와 분리된 읽기 전용 관리자 저장소다. */
@Repository @RequiredArgsConstructor
public class AdminChatRepository {
    private static final String COLLECTION = "chat_messages";
    private static final String[] FIELDS = {"scope", "gameType", "roomId", "authorId", "authorNickname", "authorAvatarId", "content", "createdAt", "acceptedAtMicros"};
    private final ReactiveMongoTemplate mongo;

    public Mono<AdminChatPage> search(AdminChatSearch search) { return page(search, search.getPage(), true); }

    private Mono<AdminChatPage> page(AdminChatSearch search, int page, boolean correctPage) {
        var criteria = Criteria.where("createdAt").gte(Date.from(search.getFrom())).lt(Date.from(search.getUntil()));
        if (search.getScope() != null) criteria.and("scope").is(search.getScope());
        else if (search.getGameCode() != null || search.getRoomId() != null) criteria.and("scope").is("GAME_ROOM");
        if (search.getGameCode() != null) criteria.and("gameType").is(search.getGameCode());
        if (search.getRoomId() != null) criteria.and("roomId").is(search.getRoomId());
        if (search.getQuery() != null) {
            Criteria nickname = Criteria.where("authorNickname").regex("^" + Pattern.quote(search.getQuery()));
            Long memberId = memberId(search.getQuery());
            criteria.andOperator(memberId == null ? nickname : new Criteria().orOperator(nickname, Criteria.where("authorId").is(memberId)));
        }
        // 접수 시각의 밀리초·마이크로초·ID 순으로 안정 정렬한다. 집계와 항목은 한 명령의 동일 입력을 공유한다.
        var aggregation = newAggregation(match(criteria), sort(Sort.by(Sort.Direction.DESC, "createdAt", "acceptedAtMicros", "_id")),
                facet(skip((long) (page - 1) * search.getSize()), limit(search.getSize()), project(FIELDS)).as("items")
                        .and(count().as("count")).as("total"))
                .withOptions(AggregationOptions.builder().maxTime(Duration.ofSeconds(3)).allowDiskUse(true).build());
        return mongo.aggregate(aggregation, COLLECTION, Document.class).single().flatMap(result -> {
            var totals = result.getList("total", Document.class);
            long total = totals.isEmpty() ? 0 : ((Number) totals.getFirst().get("count")).longValue();
            int pages = Math.toIntExact(Math.max(1, (total + search.getSize() - 1) / search.getSize()));
            if (correctPage && page > pages) return page(search, pages, false);
            return Mono.just(AdminChatPage.builder().items(result.getList("items", Document.class).stream().map(this::map).toList())
                    .page(Math.min(page, pages)).size(search.getSize()).totalPages(pages).totalElements(total).build());
        });
    }

    public Mono<AdminChatMessage> find(String id) {
        if (!id.matches("[a-f0-9]{24}")) return Mono.error(new BusinessException(ErrorCode.VALIDATION_FAILED));
        Query query = Query.query(Criteria.where("_id").is(new ObjectId(id))).maxTime(Duration.ofSeconds(3));
        query.fields().include(FIELDS);
        return mongo.findOne(query, Document.class, COLLECTION).map(this::map)
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.ADMIN_CHAT_NOT_FOUND)));
    }

    private AdminChatMessage map(Document doc) {
        // 구버전 문서도 저장된 createdAt으로 조회한다. 일반 채팅의 공개 경계 규칙은 변경하지 않는다.
        Number micros = doc.get("acceptedAtMicros", Number.class);
        var sentAt = micros != null && micros.longValue() > 0 ? ChatMessageTime.fromAcceptedAtMicros(micros.longValue()) : doc.getDate("createdAt").toInstant();
        return AdminChatMessage.builder().id(doc.getObjectId("_id").toHexString()).sentAt(sentAt)
                .authorId(number(doc, "authorId")).authorNickname(doc.getString("authorNickname")).authorAvatarId(doc.getString("authorAvatarId"))
                .scope(doc.getString("scope")).gameCode(doc.getString("gameType")).roomId(number(doc, "roomId")).content(doc.getString("content")).build();
    }
    private Long number(Document doc, String key) { Number value = doc.get(key, Number.class); return value == null ? null : value.longValue(); }
    private Long memberId(String value) {
        if (!value.matches("[1-9]\\d{0,18}")) return null;
        try { return Long.valueOf(value); } catch (NumberFormatException ignored) { return null; }
    }
}
