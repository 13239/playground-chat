package com.project.chat.global.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/** 토픽 생성 기준을 타입과 범위로 검증하며 환경마다 복제 수준을 지정한다. */
@Getter
@Validated
@ConfigurationProperties("chat.kafka")
public class ChatKafkaProperties {

    @NotBlank
    @Pattern(regexp = "[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}")
    private final String messagesTopic;

    @NotBlank
    @Pattern(regexp = "[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}")
    private final String broadcastsTopic;

    @Min(1)
    @Max(32)
    private final int partitions;

    @Min(1)
    private final short replicationFactor;

    @Min(1)
    private final int minInSyncReplicas;

    public ChatKafkaProperties(
            @DefaultValue("playground.chat.messages.v1") String messagesTopic,
            @DefaultValue("playground.chat.broadcasts.v1") String broadcastsTopic,
            @DefaultValue("3") int partitions,
            @DefaultValue("1") short replicationFactor,
            @DefaultValue("1") int minInSyncReplicas) {
        this.messagesTopic = messagesTopic;
        this.broadcastsTopic = broadcastsTopic;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.minInSyncReplicas = minInSyncReplicas;
    }

    @AssertTrue(message = "수신·전파 토픽은 달라야 하며 최소 동기 복제 수는 복제 수를 넘을 수 없습니다.")
    public boolean isTopologyValid() {
        return messagesTopic != null && !messagesTopic.equals(broadcastsTopic)
                && minInSyncReplicas <= replicationFactor;
    }
}
