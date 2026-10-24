package com.project.chat.domain.adminchat;

import com.project.chat.domain.adminchat.dto.*;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import java.time.LocalDate;

/** 메인 서버의 요청에만 응답한다. 내부 증명 필터를 통과한 저장 기록 읽기 외의 동작은 없다. */
@RestController @RequestMapping("/internal/admin/chat/messages") @RequiredArgsConstructor
public class AdminChatController {
    private final AdminChatRepository messages;
    @GetMapping
    public Mono<ResponseEntity<AdminChatPage>> search(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(required = false) String query, @RequestParam(required = false) String scope,
            @RequestParam(required = false) String gameCode, @RequestParam(required = false) Long roomId,
            @RequestParam(defaultValue = "1") int page, @RequestParam(defaultValue = "10") int size) {
        return messages.search(AdminChatSearch.of(from, to, query, scope, gameCode, roomId, page, size)).map(this::response);
    }
    @GetMapping("/{id}")
    public Mono<ResponseEntity<AdminChatMessage>> detail(@PathVariable String id) { return messages.find(id).map(this::response); }
    private <T> ResponseEntity<T> response(T value) { return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value); }
}
