package com.project.chat.domain.chat.entity;

import com.project.chat.domain.chat.enums.ChatGameType;
import com.project.chat.domain.chat.enums.ChatScope;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;

/** 채팅 이력의 영속 문서다. 작성자 정보는 인증된 사용자로부터 서버가 확정하며 수정하지 않는다. */
@Getter
@Document(collection = "chat_messages")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ChatMessage {

    @Id
    private ObjectId id;

    @Field("conversationKey")
    private String conversationKey;

    @Field("scope")
    private ChatScope scope;

    @Field("gameType")
    private ChatGameType gameType;

    @Field("roomId")
    private Long roomId;

    @Field("authorId")
    private Long authorId;

    @Field("authorNickname")
    private String authorNickname;

    @Field("authorAvatarId")
    private String authorAvatarId;

    @Field("clientMessageId")
    private String clientMessageId;

    @Field("content")
    private String content;

    @Field("createdAt")
    private Instant createdAt;

    @Builder
    private ChatMessage(String conversationKey, ChatScope scope, ChatGameType gameType, Long roomId,
                        Long authorId, String authorNickname, String authorAvatarId,
                        String clientMessageId, String content, Instant createdAt) {
        this.conversationKey = conversationKey;
        this.scope = scope;
        this.gameType = gameType;
        this.roomId = roomId;
        this.authorId = authorId;
        this.authorNickname = authorNickname;
        this.authorAvatarId = authorAvatarId;
        this.clientMessageId = clientMessageId;
        this.content = content;
        this.createdAt = createdAt;
    }
}
