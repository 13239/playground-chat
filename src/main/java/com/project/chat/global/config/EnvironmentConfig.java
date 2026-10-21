package com.project.chat.global.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Set;

/** back처럼 local/dev/prod를 독립 실행하며 환경 설정이 섞인 기동을 거절한다. */
@Configuration(proxyBeanMethods = false)
public class EnvironmentConfig {

    private static final Set<String> SUPPORTED_PROFILES = Set.of("local", "dev", "prod");

    public EnvironmentConfig(Environment environment) {
        String[] profiles = environment.getActiveProfiles();
        if (profiles.length != 1 || !SUPPORTED_PROFILES.contains(profiles[0])) {
            throw new IllegalStateException(
                    "local, dev, prod 중 하나의 프로필만 선택해 주세요. 예: --spring.profiles.active=local");
        }
    }
}
