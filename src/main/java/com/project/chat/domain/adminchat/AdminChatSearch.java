package com.project.chat.domain.adminchat;

import com.project.chat.global.exception.*;
import lombok.Builder;
import lombok.Getter;
import java.time.*;
import java.util.Set;

/** KST 날짜·작성자 스냅샷 검색만 허용하며 게임 코드는 열거형에 묶지 않는다. */
@Getter @Builder
public class AdminChatSearch {
    private final Instant from;
    private final Instant until;
    private final String query;
    private final String scope;
    private final String gameCode;
    private final Long roomId;
    private final int page;
    private final int size;

    public static AdminChatSearch of(LocalDate from, LocalDate to, String query, String scope, String gameCode, Long roomId, int page, int size) {
        ZoneId zone = ZoneId.of("Asia/Seoul");
        LocalDate today = LocalDate.now(zone), start = from == null ? today.minusDays(6) : from, end = to == null ? today : to;
        if (start.isAfter(end) || start.getYear() < 1000 || !end.isBefore(LocalDate.of(9999, 12, 31))
                || page < 1 || !Set.of(10, 20, 30, 50).contains(size)
                || query != null && query.length() > 100
                || scope != null && !Set.of("GLOBAL", "GAME_ROOM").contains(scope)
                || gameCode != null && !gameCode.matches("[A-Z][A-Z0-9_]{0,63}")
                || roomId != null && (roomId < 1 || roomId > 9_007_199_254_740_991L)
                || "GLOBAL".equals(scope) && (gameCode != null || roomId != null)) {
            throw new BusinessException(ErrorCode.VALIDATION_FAILED);
        }
        return builder().from(start.atStartOfDay(zone).toInstant()).until(end.plusDays(1).atStartOfDay(zone).toInstant())
                .query(query == null || query.isBlank() ? null : query.strip()).scope(scope).gameCode(gameCode).roomId(roomId).page(page).size(size).build();
    }
}
