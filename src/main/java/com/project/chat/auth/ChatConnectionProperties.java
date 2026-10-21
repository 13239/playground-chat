package com.project.chat.auth;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.util.Set;

/** 인증 전 예산과 실제 IP 헤더를 신뢰할 프록시를 기동 시 고정한다. 기본값은 직접 접속 전용이다. */
@Getter
@Validated
@ConfigurationProperties("chat.connection")
public class ChatConnectionProperties {
    @Min(1) @Max(64)
    private final int maxPendingPerIp;
    @Min(1) @Max(100)
    private final int burst;
    @Min(1) @Max(10)
    private final int attemptsPerSecond;
    private final Set<String> trustedProxies;

    public ChatConnectionProperties(@DefaultValue("8") int maxPendingPerIp,
                                    @DefaultValue("20") int burst,
                                    @DefaultValue("1") int attemptsPerSecond,
                                    @DefaultValue Set<String> trustedProxies) {
        this.maxPendingPerIp = maxPendingPerIp;
        this.burst = burst;
        this.attemptsPerSecond = attemptsPerSecond;
        this.trustedProxies = trustedProxies == null ? Set.of() : trustedProxies.stream()
                .filter(value -> !value.isBlank()).collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
}
