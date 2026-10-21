package com.project.chat.global.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

/** back과 동일하게 HTTP 상태와 클라이언트용 오류 식별자를 한 곳에서 관리한다. */
@Getter
@RequiredArgsConstructor
public enum ErrorCode {

    // WebFlux 입력 검증과 JSON 역직렬화 실패를 구분한다.
    VALIDATION_FAILED(HttpStatus.BAD_REQUEST, "입력값이 올바르지 않습니다."),
    INVALID_REQUEST_BODY(HttpStatus.BAD_REQUEST, "요청 본문의 형식 또는 값이 올바르지 않습니다."),
    INVALID_CHAT_DESTINATION(HttpStatus.BAD_REQUEST, "채팅 대상이 올바르지 않습니다."),

    // 업무 HTTP 경로는 차단하고 WebSocket은 Origin·일회용 증명·브라우저 키로 별도 검증한다.
    AUTHENTICATION_REQUIRED(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다."),
    INVALID_AUTHENTICATION_REQUEST(HttpStatus.BAD_REQUEST, "채팅 접속 요청 형식이 올바르지 않습니다."),
    CHAT_TICKET_INVALID(HttpStatus.UNAUTHORIZED, "채팅 접속 증명이 유효하지 않습니다. 다시 연결해 주세요."),
    CHAT_PROOF_INVALID(HttpStatus.FORBIDDEN, "브라우저의 채팅 접속 증명을 확인할 수 없습니다."),
    CHAT_ORIGIN_INVALID(HttpStatus.FORBIDDEN, "허용되지 않은 채팅 연결입니다."),
    CHAT_CONTROL_RATE_LIMIT_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, "연결 확인 요청이 너무 많습니다."),
    INVALID_SESSION(HttpStatus.UNAUTHORIZED, "유효하지 않거나 만료된 로그인 세션입니다."),
    ACCESS_DENIED(HttpStatus.FORBIDDEN, "접근 권한이 없습니다."),

    // 연결 실패의 내부 주소나 자격 증명을 응답 본문에 포함하지 않는다.
    CHAT_SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "채팅 서비스를 일시적으로 이용할 수 없습니다."),
    INTERNAL_SERVER_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "요청 처리 중 오류가 발생했습니다.");

    private final HttpStatus status;
    private final String message;
}
