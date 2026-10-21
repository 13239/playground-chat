package com.project.chat.global.exception;

import lombok.Getter;

/** 도메인 규칙 위반을 공통 ProblemDetail 응답으로 전달한다. */
@Getter
public class BusinessException extends RuntimeException {

    private final ErrorCode errorCode;

    public BusinessException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.errorCode = errorCode;
    }
}
