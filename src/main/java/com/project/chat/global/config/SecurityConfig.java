package com.project.chat.global.config;

import com.project.chat.auth.ChatAuthProperties;
import com.project.chat.global.security.ChatOriginWebFilter;
import com.project.chat.global.security.AdminChatCapabilityFilter;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import com.project.chat.global.exception.ErrorCode;
import com.project.chat.global.security.handler.SecurityProblemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;
import org.springframework.security.web.server.savedrequest.NoOpServerRequestCache;
import reactor.core.publisher.Mono;

/** 상태 확인·검증된 내부 조회·WebSocket만 열고 채팅 접속 인증은 첫 프레임에서 처리한다. */
@Configuration(proxyBeanMethods = false)
public class SecurityConfig {

    @Bean
    public ReactiveAuthenticationManager authenticationManager() {
        // 쿠키·Basic·임의의 Bearer 토큰을 HTTP 인증으로 승인하지 않으며 WebSocket 증명만 별도로 검증한다.
        return authentication -> Mono.empty();
    }

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(
            ServerHttpSecurity http, SecurityProblemWriter problemWriter, ChatAuthProperties properties,
            ReactiveStringRedisTemplate redis) {
        return http
                .addFilterBefore(new ChatOriginWebFilter(properties, problemWriter), SecurityWebFiltersOrder.AUTHENTICATION)
                // 역할은 back의 DB에서 검사하며 여기서는 그 요청의 일회용 내부 증명만 확인한다.
                .addFilterBefore(new AdminChatCapabilityFilter(redis, problemWriter), SecurityWebFiltersOrder.AUTHORIZATION)
                // 쿠키 인증·로그인 화면·서버 세션을 생성하지 않으며 브라우저용 업무 HTTP 경로는 모두 차단한다.
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .cors(ServerHttpSecurity.CorsSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .logout(ServerHttpSecurity.LogoutSpec::disable)
                .securityContextRepository(NoOpServerSecurityContextRepository.getInstance())
                .requestCache(cache -> cache.requestCache(NoOpServerRequestCache.getInstance()))
                .exceptionHandling(handler -> handler
                        .authenticationEntryPoint((exchange, exception) ->
                                problemWriter.write(exchange, ErrorCode.AUTHENTICATION_REQUIRED))
                        .accessDeniedHandler((exchange, exception) ->
                                problemWriter.write(exchange, ErrorCode.ACCESS_DENIED)))
                .authorizeExchange(authorize -> authorize
                        .pathMatchers(HttpMethod.GET, "/actuator/health",
                                "/actuator/health/liveness", "/actuator/health/readiness").permitAll()
                        // 업그레이드는 인증 완료가 아니다. AUTH 성공 전에는 업무 처리나 공개 사용자 응답을 만들지 않는다.
                        .pathMatchers(HttpMethod.GET, "/ws/chat").permitAll()
                        .pathMatchers(HttpMethod.GET, "/internal/admin/chat/messages", "/internal/admin/chat/messages/{id}", "/internal/admin/chat/health").permitAll()
                        .anyExchange().denyAll())
                .build();
    }
}
