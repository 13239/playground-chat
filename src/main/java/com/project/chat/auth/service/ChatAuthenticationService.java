package com.project.chat.auth.service;

import com.project.chat.auth.ChatAuthProperties;
import com.project.chat.auth.dto.request.ChatClientFrame;
import com.project.chat.auth.proof.ChatConnectionProofVerifier;
import com.project.chat.auth.repository.RedisChatAuthRepository;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.ChatPrincipal;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeoutException;

/** 증명 조회 → 브라우저 키 확인 → 원자적 소비를 거친 뒤에만 내부 principal을 만든다. */
@Service
@RequiredArgsConstructor
public class ChatAuthenticationService {

    private final RedisChatAuthRepository tickets;
    private final ChatConnectionProofVerifier proofVerifier;
    private final ChatAuthProperties properties;

    public Mono<ChatPrincipal> authenticate(ChatClientFrame frame) {
        if (!Integer.valueOf(1).equals(frame.getVersion()) || !"AUTH".equals(frame.getType())
                || frame.getTicket() == null || !frame.getTicket().matches("[A-Za-z0-9_-]{43}")) {
            return Mono.error(new BusinessException(ErrorCode.INVALID_AUTHENTICATION_REQUEST));
        }
        String hash = hash(frame.getTicket());
        return tickets.find(hash).flatMap(ticket -> {
            if (!properties.getOrigin().equals(ticket.getOrigin())
                    || !properties.getWebsocketUrl().equals(ticket.getWebsocketUrl())) {
                return Mono.error(new BusinessException(ErrorCode.CHAT_TICKET_INVALID));
            }
            // 서명 검증 비용을 Netty 이벤트 루프에서 분리하고 실패한 증명은 소비하지 않는다.
            return Mono.fromRunnable(() -> proofVerifier.verify(hash, ticket, frame.getProof()))
                    .subscribeOn(Schedulers.boundedElastic())
                    .then(tickets.consume(hash, ticket));
        }).timeout(Duration.ofSeconds(5));
    }

    public Mono<Void> validate(ChatPrincipal principal) {
        // 접속이나 heartbeat만으로 로그인 세션을 연장하지 않는다.
        return tickets.validate(principal).timeout(Duration.ofSeconds(4))
                .onErrorMap(TimeoutException.class, exception -> new BusinessException(ErrorCode.CHAT_SERVICE_UNAVAILABLE));
    }

    public static String hash(String value) {
        try {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.US_ASCII)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256을 사용할 수 없습니다.", exception);
        }
    }
}
