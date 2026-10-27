package com.project.chat.domain.social;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Service;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.*;
import com.project.chat.domain.chat.entity.ChatMessage;
import com.project.chat.domain.chat.dto.response.ChatMessageResponse;
import com.project.chat.domain.chat.service.ChatProfiles;
import com.project.chat.global.exception.*;

/** 로그인 시각 대신 현재 참가 세대·친구 삭제 경계로 이력/미리보기/안 읽음을 제한한다. */
@Service @RequiredArgsConstructor
public class SocialMessages {
    private final ReactiveMongoTemplate mongo;
    private final SocialAccessRepository access;
    private final ChatProfiles profiles;
    private static final Sort NEWEST=Sort.by(Sort.Order.desc("acceptedAtMicros"),Sort.Order.desc("_id"));
    private static final int SIZE=30;
    private Query visible(long user,String room,SocialAccessRepository.Access a,long until) {
        var c=Criteria.where("conversationKey").is("social:"+room).and("scope").is("SOCIAL")
            .and("acceptedAtMicros").gt(a.getSince()).lte(Math.min(until,a.getUntil()));
        if(a.getBlocked()!=null&&!a.getBlocked().isEmpty())c.and("authorId").nin(a.getBlocked());
        return Query.query(c).maxTime(Duration.ofSeconds(3));
    }
    private String marker(long user,String room,SocialAccessRepository.Access a){return user+":"+room+":"+a.getMembershipId();}
    /** 조회 도중 차단/퇴장/재참가가 발생하면 이전 범위의 본문·개수·미리보기를 보내지 않는다. */
    private Mono<SocialAccessRepository.Access> unchanged(long user,String room,SocialAccessRepository.Access before) {
        return access.require(user,room).filter(now->Objects.equals(now.getMembershipId(),before.getMembershipId())
            &&now.getSince()==before.getSince()&&now.getUntil()==before.getUntil()&&Objects.equals(now.getBlocked(),before.getBlocked()))
            .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.SOCIAL_ACCESS_DENIED)));
    }
    private Mono<Long> readAt(long user,String room,SocialAccessRepository.Access a) {
        return mongo.findById(marker(user,room,a),Document.class,"social_read_states")
            .map(d->((Number)d.getOrDefault("at",a.getSince())).longValue()).defaultIfEmpty(a.getSince());
    }
    public Mono<SocialPayload.Summary> summary(long user,String room) {
        return access.find(user,room).flatMap(a->readAt(user,room,a).flatMap(read->{
            // 같은 필드 조건을 Query에 두 번 추가하지 않고 AND 절로 경계·차단·본인 제외를 결합한다.
            Query unseen=Query.query(new Criteria().andOperator(
                Criteria.where("conversationKey").is("social:"+room),Criteria.where("scope").is("SOCIAL"),
                Criteria.where("acceptedAtMicros").gt(Math.max(read,a.getSince())).lte(a.getUntil()),
                Criteria.where("authorId").ne(user),Criteria.where("authorId").nin(a.getBlocked()==null?List.of():a.getBlocked()))).maxTime(Duration.ofSeconds(3));
            Mono<Long> count=mongo.count(unseen,ChatMessage.class);
            Mono<ChatMessage> last=mongo.findOne(visible(user,room,a,Long.MAX_VALUE).with(NEWEST).limit(1),ChatMessage.class);
            return count.flatMap(unread->last.map(message->SocialPayload.Summary.builder().conversationId(room).unread(unread)
                .preview(new String(message.getContent().codePoints().limit(120).toArray(),0,(int)message.getContent().codePoints().limit(120).count()))
                .nickname(message.getAuthorNickname()).createdAt(message.getCreatedAt()).muted(a.isMuted()).writable(a.isWritable()).build())
                .defaultIfEmpty(SocialPayload.Summary.builder().conversationId(room).unread(unread).muted(a.isMuted()).writable(a.isWritable()).build()));
        }).delayUntil(value->unchanged(user,room,a)));
    }
    public Mono<SocialPayload> summaries(long user) {
        return access.rooms(user).flatMap(room->summary(user,room).onErrorResume(BusinessException.class,error->Mono.empty()),4)
            .collectList().map(list->SocialPayload.builder().summaries(list).build());
    }
    /** 양쪽이 대화 가능한 1:1 방에서만 상대의 확인 위치를 제공한다. */
    public Mono<Long> peerReadAt(long user,String room) {
        return access.require(user,room).flatMap(a->{
            if(!"DIRECT".equals(a.getKind())||!a.isWritable())return Mono.just(0L);
            return access.members(room).filter(pair->pair.getT1()!=user).collectList()
                .flatMap(others->others.size()==1?readAt(others.getFirst().getT1(),room,others.getFirst().getT2()):Mono.just(0L))
                .delayUntil(value->unchanged(user,room,a));
        });
    }
    public Mono<ChatMessageResponse> response(ChatMessage message,long user) {
        return profiles.resolve(message.getAuthorId(),message.getAuthorNickname(),message.getAuthorAvatarId())
            .map(profile->ChatMessageResponse.from(message,user).withProfile(profile));
    }
    public Mono<SocialPayload> history(long user,String room,int page,long boundary) {
        if(page<1||page>10000)return Mono.error(new BusinessException(ErrorCode.INVALID_CHAT_HISTORY_LIMIT));
        return access.require(user,room).flatMap(a-> {
            Query query=visible(user,room,a,boundary).with(NEWEST).skip((long)(page-1)*SIZE).limit(SIZE);
            return mongo.count(visible(user,room,a,boundary),ChatMessage.class).flatMap(total->mongo.find(query,ChatMessage.class).collectList()
                .flatMap(found->unchanged(user,room,a).flatMap(current->{
                    if(!current.getMembershipId().equals(a.getMembershipId()))return Mono.error(new BusinessException(ErrorCode.SOCIAL_ACCESS_DENIED));
                    Collections.reverse(found);
                    return Flux.fromIterable(found).filter(m->current.visible(m.getAuthorId(),m.getAcceptedAtMicros()))
                        .flatMapSequential(m->response(m,user),4).collectList()
                        .flatMap(values->peerReadAt(user,room).map(read->SocialPayload.builder().conversationId(room).messages(values).page(page).pageSize(SIZE).total(total).writable(current.isWritable()).peerReadAt(read).build()));
                }))).delayUntil(value->unchanged(user,room,a));
        });
    }
    public Mono<Void> read(long user,String room,String id) {
        if(id==null||!ObjectId.isValid(id))return Mono.error(new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE));
        return access.require(user,room).flatMap(a->mongo.findById(new ObjectId(id),ChatMessage.class)
            .filter(m->("social:"+room).equals(m.getConversationKey())&&a.visible(m.getAuthorId(),m.getAcceptedAtMicros()))
            .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.SOCIAL_ACCESS_DENIED)))
            .flatMap(message->unchanged(user,room,a).then(Mono.defer(()->{
                Query marker=Query.query(Criteria.where("_id").is(marker(user,room,a)));
                Update update=new Update().max("at",message.getAcceptedAtMicros());
                return mongo.upsert(marker,update,"social_read_states")
                    .onErrorResume(DuplicateKeyException.class,error->mongo.updateFirst(marker,update,"social_read_states")).then();
            }))));
    }
    public Mono<ChatMessageResponse> status(long user,String room,UUID clientId) {
        if(clientId==null)return Mono.error(new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE));
        return access.require(user,room).flatMap(a->mongo.findOne(Query.query(Criteria.where("conversationKey").is("social:"+room)
            .and("authorId").is(user).and("clientMessageId").is(clientId.toString())),ChatMessage.class)
            .filter(m->a.visible(m.getAuthorId(),m.getAcceptedAtMicros()))
            .flatMap(m->response(m,user).delayUntil(value->unchanged(user,room,a))));
    }
}

