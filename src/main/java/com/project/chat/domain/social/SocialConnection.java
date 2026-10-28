package com.project.chat.domain.social;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.connection.SubscriptionListener;
import reactor.core.publisher.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import tools.jackson.databind.json.JsonMapper;
import com.project.chat.auth.dto.request.ChatClientFrame;
import com.project.chat.auth.dto.response.*;
import com.project.chat.auth.service.ChatAuthenticationService;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.domain.chat.enums.ChatScope;
import com.project.chat.domain.chat.kafka.*;
import com.project.chat.domain.chat.service.*;
import com.project.chat.global.security.*;
import com.project.chat.global.exception.*;

/** 인증 티켓으로 연 본인 전용 연결 하나에서 알림·대화 요약과 현재 방을 다룬다. */
@Component @RequiredArgsConstructor
public class SocialConnection {
    private final ChatAuthenticationService authentication;
    private final ChatMessageService messages;
    private final ChatKafkaPublisher publisher;
    private final ChatOutboundHub hub;
    private final ChatProfiles profiles;
    private final ChatRequestRateLimiter limiter;
    private final ChatBroadcastReadiness readiness;
    private final SocialAccessRepository access;
    private final SocialMessages socialMessages;
    private final ReactiveRedisMessageListenerContainer listener;
    private final JsonMapper mapper;
    private final ReactiveStringRedisTemplate redis;

    private ChatServerFrame frame(String type,SocialPayload payload){return ChatServerFrame.builder().version(1).type(type).social(payload).build();}
    public Flux<ChatServerFrame> responses(ChatPrincipal principal,Flux<String> incoming) {
        return hub.withSocialSubscription(events->withChanges(principal,incoming,events));
    }
    /** 15초 초대가 초기 요약 조회 사이에 유실되지 않게 Redis 구독을 먼저 확정하고 변경 신호 하나만 보관한다. */
    private Flux<ChatServerFrame> withChanges(ChatPrincipal principal,Flux<String> incoming,
            Flux<com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent> events) {
        return Flux.defer(()->{
            Sinks.Empty<Void> subscribed=Sinks.empty();
            Sinks.Many<ChatServerFrame> changes=Sinks.many().replay().latest();
            var subscription=new SubscriptionListener() {
                @Override public void onChannelSubscribed(byte[] channel,long count){subscribed.tryEmitEmpty();}
            };
            // using이 구독 전 취소·인증 종료·오류에서도 Redis 구독을 해제한다. receiveLater의 취소 공백을 만들지 않는다.
            return Flux.using(()->listener.receive(List.of(ChannelTopic.of("social:changed:v1")),subscription)
                    .filter(message->Long.toString(principal.getUserId()).equals(message.getMessage()))
                    .subscribe(message->changes.tryEmitNext(frame("SOCIAL_CHANGED",null)),error->{
                        subscribed.tryEmitError(error);changes.tryEmitError(error);
                    }),
                ignored->subscribed.asMono().thenMany(responses(principal,incoming,events,changes.asFlux())),
                reactor.core.Disposable::dispose);
        });
    }
    private Flux<ChatServerFrame> responses(ChatPrincipal principal,Flux<String> incoming,
            Flux<com.project.chat.domain.chat.dto.internal.ChatBroadcastEvent> events,Flux<ChatServerFrame> changes) {
        long user=principal.getUserId();
        AtomicReference<String> active=new AtomicReference<>();
        AtomicReference<String> historyRoom=new AtomicReference<>();
        AtomicLong boundary=new AtomicLong();
        AtomicInteger historyPage=new AtomicInteger(1);
        AtomicLong lastPing=new AtomicLong();
        Sinks.Empty<Void> closed=Sinks.empty();
        ChatServerFrame ready=ChatServerFrame.builder().version(1).type("AUTHENTICATED").connectionId(UUID.randomUUID()).user(ChatUserResponse.from(principal)).build();
        Flux<ChatServerFrame> controls=incoming.concatMap(raw->{
            ChatClientFrame c;
            try {c=mapper.readValue(raw,ChatClientFrame.class);}catch(RuntimeException error){return Mono.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));}
            if(!validControl(c))
                return Mono.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            Mono<ChatServerFrame> result=authentication.validate(principal).then(Mono.defer(()->switch(c.getType()) {
                case "PING" -> {
                    long now=System.nanoTime();long previous=lastPing.getAndSet(now);
                    if(previous!=0&&now-previous<Duration.ofSeconds(1).toNanos())yield Mono.error(new BusinessException(ErrorCode.CHAT_CONTROL_RATE_LIMIT_EXCEEDED));
                    yield Mono.just(ChatServerFrame.builder().version(1).type("PONG").build());
                }
                case "SOCIAL_SYNC" -> limiter.read(user).then(socialMessages.summaries(user)).map(p->frame("SOCIAL_SNAPSHOT",p));
                case "SOCIAL_CLOSE" -> {active.set(null);yield Mono.empty();}
                case "SOCIAL_OPEN","SOCIAL_HISTORY" -> limiter.read(user).flatMap(now->{
                    String room=SocialAccessRepository.id(c.getConversationId());
                    int page=c.getPage()==null?1:c.getPage();
                    // 최소화 뒤 과거 페이지를 복원하면 기존 조회 경계를 유지해 페이지 밀림을 막는다.
                    if(c.getType().equals("SOCIAL_OPEN")) {
                        if(page==1||!room.equals(historyRoom.get()))boundary.set(now);
                        historyRoom.set(room);
                    }
                    else if(!room.equals(active.get()))return Mono.error(new BusinessException(ErrorCode.SOCIAL_ACCESS_DENIED));
                    // 최신 페이지 재조회와 과거 이력 진입 시점에만 경계를 갱신한다.
                    // 주기 동기화가 방을 연 뒤 받은 메시지를 이전 경계로 지우지 않게 한다.
                    else if(page==1||historyPage.get()==1)boundary.set(now);
                    return socialMessages.history(user,room,page,boundary.get()).map(p->{active.set(room);historyPage.set(page);return frame("SOCIAL_HISTORY",p);});
                });
                case "SOCIAL_SEND" -> messages.prepare(principal,ChatDestination.builder().scope(ChatScope.SOCIAL)
                        .conversationId(SocialAccessRepository.id(c.getConversationId())).build(),c.getClientMessageId(),c.getContent())
                    .flatMap(command->publisher.enqueue(command).thenReturn(ChatServerFrame.builder().version(1).type("CHAT_ACCEPTED").clientMessageId(c.getClientMessageId()).build()));
                case "SOCIAL_READ" -> limiter.read(user).then(socialMessages.read(user,SocialAccessRepository.id(c.getConversationId()),c.getMessageId()))
                    .then(redis.convertAndSend("social:reads:v1",c.getConversationId()))
                    .then(socialMessages.summary(user,c.getConversationId())).map(p->frame("SOCIAL_SUMMARY",SocialPayload.builder().summaries(List.of(p)).build()));
                case "SOCIAL_STATUS" -> limiter.read(user).then(socialMessages.status(user,SocialAccessRepository.id(c.getConversationId()),c.getClientMessageId()))
                    .map(m->ChatServerFrame.builder().version(1).type("CHAT_STATUS_RESULT").stored(true).message(m).clientMessageId(c.getClientMessageId()).build())
                    .defaultIfEmpty(ChatServerFrame.builder().version(1).type("CHAT_STATUS_RESULT").stored(false).clientMessageId(c.getClientMessageId()).build());
                default -> Mono.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
            }));
            // 조회 제한도 연결을 유지하며 대기 시간·대상 방·페이지를 돌려줘 이전 화면의 재시도를 막는다.
            return result.onErrorResume(BusinessException.class,error->Mono.just(ChatServerFrame.builder().version(1).type("CHAT_ERROR")
                .operation(c.getType()).code(error.getErrorCode().name()).clientMessageId(c.getClientMessageId())
                .retryAfterMillis(error instanceof ChatRequestLimitException limited?limited.getRetryAfterMillis():null)
                .social(c.getConversationId()==null?null:SocialPayload.builder().conversationId(c.getConversationId()).page(c.getPage()).build()).build()));
        },1).doOnComplete(()->closed.tryEmitEmpty());

        Flux<ChatServerFrame> live=events.concatMap(event->access.find(user,event.getConversationId())
            .filter(a->a.visible(event)).flatMap(a->authentication.validate(principal).then(socialMessages.summary(user,event.getConversationId()))
                .flatMap(summary->{
                    Mono<ChatServerFrame> activity=Mono.just(frame("SOCIAL_ACTIVITY",SocialPayload.builder().conversationId(event.getConversationId()).summaries(List.of(summary)).build()));
                    if(!event.getConversationId().equals(active.get())||historyPage.get()!=1)return activity;
                    return profiles.resolve(event.getAuthorId(),event.getNickname(),event.getAvatarId())
                        .map(profile->ChatServerFrame.builder().version(1).type("CHAT_MESSAGE").message(event.toViewer(user).withProfile(profile))
                            .social(SocialPayload.builder().conversationId(event.getConversationId()).summaries(List.of(summary)).build()).build());
                })).onErrorResume(BusinessException.class,error->Mono.empty()),1);

        Flux<ChatServerFrame> receipts=listener.receive(ChannelTopic.of("social:reads:v1"))
            .filter(message->message.getMessage().equals(active.get()))
            .concatMap(message->authentication.validate(principal).then(socialMessages.peerReadAt(user,message.getMessage()))
                .map(read->frame("SOCIAL_RECEIPT",SocialPayload.builder().conversationId(message.getMessage()).peerReadAt(read).build()))
                .onErrorResume(BusinessException.class,error->Mono.empty()),1);
        Mono<Void> validation=Flux.interval(Duration.ZERO,Duration.ofSeconds(5)).concatMap(tick->
            authentication.validate(principal).then(profiles.resolve(user,principal.getNickname(),principal.getAvatarId()))
                .flatMap(profile->access.online(profile.getPublicUserId())),1).then();
        Flux<ChatServerFrame> fallback=Flux.interval(Duration.ofSeconds(30)).map(tick->frame("SOCIAL_CHANGED",null));
        return Flux.concat(Mono.just(ready),socialMessages.summaries(user).map(p->frame("SOCIAL_SNAPSHOT",p)),
            Flux.merge(controls,live,changes,receipts,fallback))
            .takeUntilOther(Mono.firstWithSignal(closed.asMono(),validation,readiness.unavailableSignal()));
    }

    /** 연결 인증·공용 채팅의 필드를 섞거나 동작과 무관한 식별자를 전달하는 프레임은 거절한다. */
    private boolean validControl(ChatClientFrame c) {
        if(c.getType()==null||!Integer.valueOf(1).equals(c.getVersion())||c.getTicket()!=null||c.getProof()!=null
            ||c.getScope()!=null||c.getGameType()!=null||c.getRoomId()!=null||c.getBefore()!=null||c.getLimit()!=null
            ||c.getRequestId()!=null||c.getOnlineCursor()!=null||c.getOnlineLimit()!=null)return false;
        return switch(c.getType()) {
            case "PING","SOCIAL_SYNC","SOCIAL_CLOSE" -> c.getConversationId()==null&&c.getPage()==null&&c.getClientMessageId()==null&&c.getContent()==null&&c.getMessageId()==null;
            case "SOCIAL_OPEN","SOCIAL_HISTORY" -> c.getConversationId()!=null&&c.getClientMessageId()==null&&c.getContent()==null&&c.getMessageId()==null;
            case "SOCIAL_SEND" -> c.getConversationId()!=null&&c.getClientMessageId()!=null&&c.getContent()!=null&&c.getPage()==null&&c.getMessageId()==null;
            case "SOCIAL_READ" -> c.getConversationId()!=null&&c.getMessageId()!=null&&c.getPage()==null&&c.getClientMessageId()==null&&c.getContent()==null;
            case "SOCIAL_STATUS" -> c.getConversationId()!=null&&c.getClientMessageId()!=null&&c.getPage()==null&&c.getMessageId()==null&&c.getContent()==null;
            default -> false;
        };
    }
}

