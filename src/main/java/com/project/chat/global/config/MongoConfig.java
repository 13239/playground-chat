package com.project.chat.global.config;

import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/** Boot 4의 reactive Mongo 자동 설정을 사용하고 연결 대기 시간만 제한한다. */
@Configuration(proxyBeanMethods = false)
public class MongoConfig {

    @Bean
    public MongoClientSettingsBuilderCustomizer mongoTimeoutCustomizer() {
        // 드라이버의 재시도가 무기한처럼 보이지 않도록 연결·서버 선택·읽기 시간을 제한한다.
        return builder -> builder
                .applyToClusterSettings(settings -> settings.serverSelectionTimeout(3, TimeUnit.SECONDS))
                .applyToSocketSettings(settings -> settings
                        .connectTimeout(3, TimeUnit.SECONDS)
                        .readTimeout(5, TimeUnit.SECONDS));
    }
}
