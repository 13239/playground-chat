package com.project.chat.global.exception;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebInputException;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Servlet 전용 예외 대신 WebFlux의 바인딩·입력 예외를 공통 오류 형식으로 변환한다. */
@RestControllerAdvice
// Boot의 기본 ProblemDetail 처리기보다 먼저 공통 code와 검증 응답을 작성한다.
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GlobalExceptionHandler {

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<ProblemDetail> methodValidationFailed(HandlerMethodValidationException exception) {
        // 요청 매개변수 오류와 서버 반환값 검증 오류의 HTTP 상태를 혼동하지 않는다.
        ErrorCode code = exception.isForReturnValue()
                ? ErrorCode.INTERNAL_SERVER_ERROR : ErrorCode.VALIDATION_FAILED;
        return response(ApiProblemFactory.create(code));
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<ProblemDetail> validationFailed(WebExchangeBindException exception) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError error : exception.getFieldErrors()) {
            String message = Objects.requireNonNullElse(error.getDefaultMessage(), "유효하지 않은 값입니다.");
            errors.merge(error.getField(), message, (first, next) -> first + "\n" + next);
        }
        ProblemDetail problem = ApiProblemFactory.create(ErrorCode.VALIDATION_FAILED);
        problem.setProperty("errors", errors);
        return response(problem);
    }

    @ExceptionHandler(ServerWebInputException.class)
    public ResponseEntity<ProblemDetail> invalidRequestBody(ServerWebInputException exception) {
        return response(ApiProblemFactory.create(ErrorCode.INVALID_REQUEST_BODY));
    }

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ProblemDetail> businessException(BusinessException exception) {
        return response(ApiProblemFactory.create(exception.getErrorCode()));
    }

    @ExceptionHandler({DataAccessResourceFailureException.class, TransientDataAccessException.class})
    public ResponseEntity<ProblemDetail> infrastructureUnavailable(Exception exception) {
        // 드라이버 예외의 서버 주소·인증 정보는 클라이언트에 전달하지 않는다.
        return response(ApiProblemFactory.create(ErrorCode.CHAT_SERVICE_UNAVAILABLE));
    }

    private ResponseEntity<ProblemDetail> response(ProblemDetail problem) {
        return ResponseEntity.status(problem.getStatus())
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .cacheControl(CacheControl.noStore())
                .body(problem);
    }
}
