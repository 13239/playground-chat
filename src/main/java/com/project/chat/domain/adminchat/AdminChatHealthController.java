package com.project.chat.domain.adminchat;

import com.project.chat.domain.adminchat.dto.AdminChatHealth;
import lombok.RequiredArgsConstructor;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/** 일회용 내부 증명 필터를 통과한 back만 상태를 읽으며 상태 실패도 조회 결과로 돌려준다. */
@RestController @RequiredArgsConstructor
public class AdminChatHealthController {
    private final AdminChatHealthService health;
    @GetMapping("/internal/admin/chat/health")
    public Mono<ResponseEntity<AdminChatHealth>> status() { return health.status().map(value -> ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value)); }
}
