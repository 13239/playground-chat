package com.project.chat.auth.proof;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jwt.SignedJWT;
import com.project.chat.auth.ChatAuthProperties;
import com.project.chat.auth.dto.internal.ChatTicket;
import com.project.chat.global.exception.BusinessException;
import com.project.chat.global.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;

/** local/prod에서는 로그인 때 등록한 추출 불가 브라우저 키의 소유도 함께 확인한다. */
@Component
@RequiredArgsConstructor
public class ChatConnectionProofVerifier {

    private final ChatAuthProperties properties;

    public void verify(String hash, ChatTicket ticket, String value) {
        if (!properties.isProofRequired()) {
            // 기존 back의 dev HTTP 세션만 예외로 인정하며 local/prod 키 세션을 강등하지 않는다.
            if (!"dev-http".equals(ticket.getKeyThumbprint()) || value != null) throw invalid();
            return;
        }
        try {
            if (value == null || value.length() > 2048) throw invalid();
            SignedJWT proof = SignedJWT.parse(value);
            var header = proof.getHeader();
            if (!JWSAlgorithm.ES256.equals(header.getAlgorithm())
                    || !new JOSEObjectType("chat-connection-proof+jwt").equals(header.getType())
                    || header.getCriticalParams() != null || !(header.getJWK() instanceof ECKey key)
                    || key.isPrivate() || !Curve.P_256.equals(key.getCurve())
                    || !same(key.computeThumbprint().toString(), ticket.getKeyThumbprint())
                    || !proof.verify(new ECDSAVerifier(key))) throw invalid();

            var claims = proof.getJWTClaimsSet();
            Instant now = Instant.now();
            var issued = claims.getIssueTime();
            if (!same(hash, claims.getStringClaim("ticketHash"))
                    || !properties.getWebsocketUrl().equals(claims.getStringClaim("htu"))
                    || issued == null || issued.toInstant().isAfter(now.plusSeconds(5))
                    || issued.toInstant().isBefore(ticket.getIssuedAt().minusSeconds(5))
                    || claims.getJWTID() == null
                    || !claims.getJWTID().matches("[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}")) {
                throw invalid();
            }
        } catch (BusinessException exception) {
            throw exception;
        } catch (Exception exception) {
            // JOSE 예외에 포함될 수 있는 서명·원문 증명을 응답이나 로그에 남기지 않는다.
            throw invalid();
        }
    }

    private static boolean same(String first, String second) {
        return first != null && second != null && MessageDigest.isEqual(
                first.getBytes(StandardCharsets.US_ASCII), second.getBytes(StandardCharsets.US_ASCII));
    }

    private static BusinessException invalid() {
        return new BusinessException(ErrorCode.CHAT_PROOF_INVALID);
    }
}
