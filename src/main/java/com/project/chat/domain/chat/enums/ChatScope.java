package com.project.chat.domain.chat.enums;

/** 홈 공용 채팅 한 개와 게임 참가자 전용 방 채팅의 대상을 구분한다. */
public enum ChatScope {
    GLOBAL,
    GAME_ROOM,
    // 로그인 이후에도 보존하는 개인·그룹 대화와 본인 전용 소셜 연결이다.
    SOCIAL
}
