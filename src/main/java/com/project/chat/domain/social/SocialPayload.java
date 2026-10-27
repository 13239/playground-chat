package com.project.chat.domain.social;
import lombok.Builder;
import lombok.Getter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.project.chat.domain.chat.dto.response.ChatMessageResponse;
import java.time.Instant;
import java.util.List;

/** 본인 전용 소셜 프레임의 제한된 페이로드다. 내부 회원 ID·권한 캐시 원문은 보내지 않는다. */
@Getter @Builder @JsonInclude(JsonInclude.Include.NON_NULL)
public class SocialPayload {
    private final String conversationId;
    private final List<Summary> summaries;
    private final List<ChatMessageResponse> messages;
    private final Integer page;
    private final Integer pageSize;
    private final Long total;
    private final Boolean writable;
    private final Long peerReadAt;
    @Getter @Builder @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Summary {
        private final String conversationId;
        private final long unread;
        private final String preview;
        private final String nickname;
        private final Instant createdAt;
        private final boolean muted;
        private final boolean writable;
    }
}

