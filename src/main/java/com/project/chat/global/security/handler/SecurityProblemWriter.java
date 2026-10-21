package com.project.chat.global.security.handler;

import com.project.chat.global.exception.ApiProblemFactory;
import com.project.chat.global.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;

/** WebFilter의 인증·접근 거부도 컨트롤러와 같은 ProblemDetail 형식으로 작성한다. */
@Component
@RequiredArgsConstructor
public class SecurityProblemWriter {

    private final JsonMapper jsonMapper;

    public Mono<Void> write(ServerWebExchange exchange, ErrorCode errorCode) {
        return Mono.defer(() -> {
            var response = exchange.getResponse();
            if (response.isCommitted()) {
                return Mono.empty();
            }

            ProblemDetail problem = ApiProblemFactory.create(errorCode);
            // 쿼리에 포함될 수 있는 접속 증명이나 토큰을 오류 응답에 복사하지 않는다.
            problem.setInstance(URI.create(exchange.getRequest().getPath().value()));
            byte[] body = jsonMapper.writeValueAsBytes(problem);

            response.setStatusCode(errorCode.getStatus());
            response.getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
            response.getHeaders().setCacheControl(CacheControl.noStore());
            return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
        });
    }
}
