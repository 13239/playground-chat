package com.project.chat.global.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.UUID;

/** 저장 소비는 고정 그룹으로 분산하고, fan-out 소비는 실행 중인 서버마다 독립 그룹을 사용한다. */
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class ChatKafkaListenerConfig {

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
