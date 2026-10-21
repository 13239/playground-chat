package com.project.chat.global.exception;

import org.springframework.http.ProblemDetail;

/** back의 ProblemDetail + code 응답 계약을 WebFlux에서도 동일하게 사용한다. */
public final class ApiProblemFactory {

    private ApiProblemFactory() {
    }

    public static ProblemDetail create(ErrorCode errorCode) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                errorCode.getStatus(), errorCode.getMessage());
        // 화면은 변경될 수 있는 안내 문구 대신 code로 오류를 구분한다.
        problem.setProperty("code", errorCode.name());
        return problem;
    }
}
