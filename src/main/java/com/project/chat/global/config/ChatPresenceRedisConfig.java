package com.project.chat.global.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;

/** Redis Pub/Sub 전용 연결을 Spring Bean 수명주기로 관리해 서버 종료 시 안전하게 해제한다. */
@Configuration(proxyBeanMethods = false)
public class ChatPresenceRedisConfig {

    @Bean
    public ReactiveRedisMessageListenerContainer chatPresenceListenerContainer(
            ReactiveRedisConnectionFactory connectionFactory) {
        // Container는 생성 즉시 Redis 연결을 확보하므로 반드시 Spring이 destroy를 호출하도록 한다.
        return new ReactiveRedisMessageListenerContainer(connectionFactory);
    }
}
