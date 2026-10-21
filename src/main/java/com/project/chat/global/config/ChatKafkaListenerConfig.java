package com.project.chat.global.config;

import com.project.chat.domain.chat.kafka.ChatMessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.util.backoff.FixedBackOff;

import java.util.UUID;

/** 저장 소비는 고정 그룹으로 분산하고, fan-out 소비는 실행 중인 서버마다 독립 그룹을 사용한다. */
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class ChatKafkaListenerConfig {

    @Bean
    public SmartInitializingSingleton chatConsumerProcessingBudget(ConsumerFactory<?, ?> consumerFactory) {
        return () -> {
            // 환경 변수·공통 Kafka 속성으로 덮어써도 실제 consumer 설정의 최악 처리 시간이 poll 안에 들어야 한다.
            var config = consumerFactory.getConfigurationProperties();
            long records = Long.parseLong(String.valueOf(config.getOrDefault(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500)));
            long interval = Long.parseLong(String.valueOf(config.getOrDefault(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000)));
            if (records < 1 || records > (interval - 60_000) / ChatMessageProcessor.MAX_PROCESSING_TIME.toMillis()) {
                throw new IllegalArgumentException("Kafka poll 제한 안에 채팅 저장과 60초의 커밋·재시도 여유가 필요합니다. max-poll-records를 줄여 주세요.");
            }
        };
    }

    @Bean
    public String chatBroadcastGroupId() {
        // 새로운 프로세스는 과거 방송을 재전파하지 않고 접속 중 발생한 메시지만 수신한다.
        return "playground-chat-live-" + UUID.randomUUID();
    }

    @Bean
    public DefaultErrorHandler chatKafkaErrorHandler() {
        // MongoDB나 Kafka의 일시 장애에서 offset을 넘기지 않는다. 독성 데이터는 소비자에서 별도로 검증·폐기한다.
        return new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
