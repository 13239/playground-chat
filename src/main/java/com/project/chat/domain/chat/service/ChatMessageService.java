package com.project.chat.domain.chat.service;

import com.project.chat.auth.service.ChatAuthenticationService;
import com.project.chat.domain.chat.dto.internal.ChatDestination;
import com.project.chat.domain.chat.dto.internal.ChatSendCommand;
import com.project.chat.domain.chat.dto.response.ChatHistoryResponse;
import com.project.chat.domain.chat.dto.response.ChatMessageResponse;
import com.project.chat.domain.chat.entity.ChatMessage;
import com.project.chat.domain.chat.repository.ChatMessageHistoryRepository;
import com.project.chat.domain.chat.repository.ChatMessageRepository;
import com.project.chat.domain.chat.support.ChatMessageTime;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import com.project.chat.global.security.ChatRequestRateLimiter;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** 인증된 발신자의 메시지를 멱등 저장하고 같은 대화의 이전 이력만 커서로 조회한다. */
@Service
@RequiredArgsConstructor
public class ChatMessageService {

    private static final int DEFAULT_PAGE_SIZE = 30;
    private static final int MAX_PAGE_SIZE = 50;

    private final ChatAuthenticationService authentication;
    private final ChatDestinationAccessPolicy accessPolicy;
    private final ChatMessageRepository repository;
    private final ChatMessageHistoryRepository historyRepository;
    private final ChatRequestRateLimiter rateLimiter;

    public Mono<ChatMessageResponse> save(ChatPrincipal principal, ChatDestination destination,
                                          UUID clientMessageId, String content) {
        // 기존 3단계 서비스 사용 경로도 유지한다. 저장 전 인증·대상 권한을 재확인한다.
        return prepare(principal, destination, clientMessageId, content)
                .flatMap(this::saveAccepted)
                // 이전 로그인/구버전 문서와 UUID가 같아도 과거 본문을 동기 응답으로 공개하지 않는다.
                .filter(saved -> saved.getAcceptedAtMicros() != null
                        && saved.getAcceptedAtMicros() > principal.getMessageBoundaryMicros())
                .switchIfEmpty(Mono.error(new BusinessException(ErrorCode.CHAT_MESSAGE_CONFLICT)))
                .map(saved -> ChatMessageResponse.from(saved, principal.getUserId()));
    }

    /** WS 요청을 Kafka로 접수하기 전에 실제 세션·채팅 대상·요청 내용을 검증한다. */
    public Mono<ChatSendCommand> prepare(ChatPrincipal principal, ChatDestination destination,
                                          UUID clientMessageId, String content) {
        return Mono.defer(() -> {
            String validated = ChatMessageContentValidator.validate(content);
            if (clientMessageId == null || principal == null || destination == null) {
                return Mono.error(new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE));
            }
            // 클라이언트의 작성자·시각·대화키는 받지 않는다. 현재 인증 주체로만 확정한다.
            return authentication.validate(principal)
                    .then(accessPolicy.authorize(principal, destination))
                    .then(rateLimiter.send(principal.getUserId()))
                    .map(acceptedAt -> ChatSendCommand.builder()
                            .version(1)
                            .clientMessageId(clientMessageId)
                            .scope(destination.getScope())
                            .gameType(destination.getGameType())
                            .roomId(destination.getRoomId())
                            .authorId(principal.getUserId())
                            .authorNickname(principal.getNickname())
                            .authorAvatarId(principal.getAvatarId())
                            .content(validated)
                            .acceptedAtMicros(acceptedAt)
                            .build());
        });
    }

    /** Kafka 내부 소비 전용: 진입 시 이미 세션을 확인했으며 소비 지연 중 세션이 만료돼도 접수한 글을 저장한다. */
    public Mono<ChatMessage> saveAccepted(ChatSendCommand command) {
        return Mono.defer(() -> {
            if (command == null || command.getVersion() != 1 || command.getClientMessageId() == null
                    || command.getAuthorId() == null || command.getAuthorId() <= 0
                    || command.getAuthorNickname() == null || command.getAuthorNickname().isBlank()
                    || command.getAuthorNickname().length() > 150
                    || command.getAuthorAvatarId() == null || command.getAuthorAvatarId().length() > 100
                    || command.getAcceptedAtMicros() != null && command.getAcceptedAtMicros() <= 0) {
                return Mono.error(new BusinessException(ErrorCode.INVALID_CHAT_MESSAGE));
            }
            String validated = ChatMessageContentValidator.validate(command.getContent());
            // 배포 전 Kafka에 접수된 구버전 요청은 유실하지 않고 보존한다. 시각이 없으므로 공개 범위에는 넣지 않는다.
            long acceptedAtMicros = command.getAcceptedAtMicros() == null ? 0 : command.getAcceptedAtMicros();
            ChatDestination destination = ChatDestination.builder()
                    .scope(command.getScope()).gameType(command.getGameType()).roomId(command.getRoomId()).build();
            // 외부 클라이언트는 이 함수를 호출할 수 없다. Kafka 토픽 쓰기 권한은 chat 서비스로 제한해야 한다.
            ChatPrincipal sender = ChatPrincipal.builder()
                    .userId(command.getAuthorId()).nickname(command.getAuthorNickname())
                    .avatarId(command.getAuthorAvatarId()).build();
            // 접수할 때 방 참가 권한을 검증했다. Kafka 처리 전에 퇴장해도 이미 접수한 메시지는 보존한다.
            // 수신·이력 공개는 현재 참가자와 입장 경계를 별도로 재검증한다.
            return persist(destination, command.getClientMessageId(), validated, sender, acceptedAtMicros);
        });
    }

    private Mono<ChatMessage> persist(ChatDestination destination, UUID clientMessageId,
                                      String content, ChatPrincipal principal, long acceptedAtMicros) {
        String key = destination.getConversationKey();
        // 재수신 시 동일한 UUID의 원본 문서를 사용한다. 서로 다른 내용은 명시적으로 충돌 처리한다.
        ChatMessage message = ChatMessage.builder()
                .conversationKey(key)
                .scope(destination.getScope())
                .gameType(destination.getGameType())
                .roomId(destination.getRoomId())
                .authorId(principal.getUserId())
                .authorNickname(principal.getNickname())
                .authorAvatarId(principal.getAvatarId())
                .clientMessageId(clientMessageId.toString())
                .content(content)
                // 화면 시각과 공개 범위 모두 최초 접수를 기준으로 하며 재처리 시 최초 문서를 유지한다.
                .createdAt(acceptedAtMicros == 0 ? Instant.now()
                        : ChatMessageTime.fromAcceptedAtMicros(acceptedAtMicros))
                .acceptedAtMicros(acceptedAtMicros)
                .build();
        return repository.save(message)
                .onErrorResume(DuplicateKeyException.class, exception ->
                        repository.findByConversationKeyAndAuthorIdAndClientMessageId(
                                        key, principal.getUserId(), clientMessageId.toString())
                                .switchIfEmpty(Mono.error(exception))
                                .flatMap(existing -> existing.getContent().equals(content)
                                        ? Mono.just(existing)
                                        : Mono.error(new BusinessException(ErrorCode.CHAT_MESSAGE_CONFLICT))));
    }

    public Mono<ChatHistoryResponse> history(ChatPrincipal principal, ChatDestination destination,
                                             String before, Integer requestedSize) {
        return Mono.defer(() -> {
            int size = requestedSize == null ? DEFAULT_PAGE_SIZE : requestedSize;
            if (size < 1 || size > MAX_PAGE_SIZE) {
                return Mono.error(new BusinessException(ErrorCode.INVALID_CHAT_HISTORY_LIMIT));
            }
            ChatMessageCursor cursor = before == null ? null : ChatMessageCursor.decode(before);
            return authentication.validate(principal)
                    .then(accessPolicy.authorize(principal, destination))
                    .then(rateLimiter.read(principal.getUserId()))
                    // limit+1만 조회해 이전 페이지가 있는지 판단하며 요청마다 전체 개수를 세지 않는다.
                    .then(historyRepository.findRecent(destination.getConversationKey(),
                                    principal.getMessageBoundaryMicros(),
                                    cursor == null ? null : cursor.getAcceptedAtMicros(),
                                    cursor == null ? null : cursor.getId(), size + 1)
                            .collectList())
                    .map(results -> toPage(results, size, principal.getUserId()));
        });
    }

    /** 최근 페이지와 무관하게 본인의 전송 UUID만 확인한다. 다른 사용자나 이전 로그인 내용은 반환하지 않는다. */
    public Mono<ChatMessageResponse> status(ChatPrincipal principal, ChatDestination destination, UUID clientMessageId) {
        return authentication.validate(principal)
                .then(accessPolicy.authorize(principal, destination))
                .then(rateLimiter.read(principal.getUserId()))
                .then(repository.findByConversationKeyAndAuthorIdAndClientMessageId(
                        destination.getConversationKey(), principal.getUserId(), clientMessageId.toString()))
                .filter(message -> message.getAcceptedAtMicros() != null
                        && message.getAcceptedAtMicros() > principal.getMessageBoundaryMicros())
                .map(message -> ChatMessageResponse.from(message, principal.getUserId()));
    }

    private ChatHistoryResponse toPage(List<ChatMessage> results, int size, Long viewerId) {
        boolean hasMore = results.size() > size;
        List<ChatMessage> page = new ArrayList<>(results.subList(0, Math.min(size, results.size())));
        String nextCursor = hasMore ? ChatMessageCursor.encode(page.getLast()) : null;
        // MongoDB 조회는 최신순으로 수행하지만 화면은 과거 → 최신순으로 출력한다.
        Collections.reverse(page);
        return ChatHistoryResponse.builder()
                .messages(page.stream().map(message -> ChatMessageResponse.from(message, viewerId)).toList())
                .hasMore(hasMore)
                .nextCursor(nextCursor)
                .build();
    }
}
