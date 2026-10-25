package com.project.chat.domain.chat.support;

import java.util.Set;

/** 새 카탈로그 밖의 과거 메시지·Redis 프로필은 시안 5번으로 표시하고 새 식별자는 그대로 전달한다. */
public final class ChatAvatarCatalog {
    public static final String DEFAULT = "SPROUT";
    private static final Set<String> IDS = Set.of(
            "SPROUT",
            "CLOUD",
            "JELLY_STAR",
            "LILAC_ROBOT",
            "HOODIE_PENGUIN",
            "HEADPHONE_DINO",
            "SEAL",
            "RAIN_HAT_FROG",
            "RAINDROP",
            "WIZARD_MOCHI",
            "MINT_ROBOT",
            "HOODIE_HAMSTER",
            "HEADPHONE_DRAGON",
            "HEADPHONE_ALIEN",
            "CREAM_ROBOT",
            "PEACH_SPROUT",
            "BLUE_OWL",
            "GAMER_GHOST",
            "HEADPHONE_WHALE",
            "SNOWMAN");

    private ChatAvatarCatalog() {
    }

    /** 저장된 채팅 본문·시각은 건드리지 않고 공개 응답의 폐기된 이미지 식별자만 정규화한다. */
    public static String normalize(String avatarId) {
        return avatarId != null && IDS.contains(avatarId) ? avatarId : DEFAULT;
    }
}
