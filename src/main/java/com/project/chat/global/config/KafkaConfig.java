package com.project.chat.global.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Kafka 수신 토픽과 저장 완료 후 전파할 토픽을 분리해 다중 서버 확장을 준비한다. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ChatKafkaProperties.class)
public class KafkaConfig {

    @Bean
    public KafkaAdmin.NewTopics chatTopics(ChatKafkaProperties properties) {
        // 토픽만 준비하며 메시지 발행·저장·전파 소비자는 4단계에서 구현한다.
        return new KafkaAdmin.NewTopics(
                topic(properties.getMessagesTopic(), properties),
                topic(properties.getBroadcastsTopic(), properties));
    }

    private NewTopic topic(String name, ChatKafkaProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicationFactor())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        Integer.toString(properties.getMinInSyncReplicas()))
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "16384")
                .build();
    }

    @Bean(destroyMethod = "close")
    public Admin chatKafkaAdminClient(KafkaAdmin kafkaAdmin) {
        // 상태 확인마다 스레드·연결을 만들지 않고 같은 비동기 Admin 클라이언트를 재사용한다.
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }
}
