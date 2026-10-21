# Playground Chat

Spring Boot 4.1.1·Java 25·WebFlux 기반의 독립 채팅 서비스다. MongoDB에는 대화 이력을 저장하고 Kafka로 메시지를 처리·전파하며 Redis는 접속 상태와 기존 로그인 세션 연동에 사용한다.

현재는 **4단계: WebSocket·Kafka 메시지 수신·저장 후 실시간 전파**까지 코드가 작성되어 있다. WebSocket 인증 후 홈 단일 채널에 메시지를 발행하고 과거 이력을 조회할 수 있다. 화면 실연동과 Redis 접속자 수·목록은 5~6단계에서 구현한다. 게임 방은 참가 권한 검증이 연결될 때까지 저장·조회·전송을 차단한다.

## 프로젝트 구조

back과 동일한 auth / domain / global 구분과 controller → service → repository 계층을 따른다. 이번 단계에 실제 역할이 있는 클래스만 만들고 나머지 계층은 해당 기능을 구현할 때 추가한다.

```text
com.project.chat
├─ auth                         일회용 접속 증명·브라우저 키·인증 WebSocket/채팅 프레임
│  ├─ dto                       builder 방식의 인증·제어 프레임
│  ├─ proof                     로그인 키의 ES256 소유 증명
│  ├─ repository                reactive Redis 증명 소비·세션 확인
│  ├─ service                   인증과 현재 세션 검증
│  └─ websocket                 인증·PING/PONG·수신/이력/실시간 전파 연결 생명주기
├─ domain
│  ├─ chat
│  │  ├─ dto/internal           builder 방식의 검증된 채팅 대상
│  │  ├─ dto/response           저장 메시지·커서 이력 응답 DTO
│  │  ├─ entity                 MongoDB chat_messages 문서
│  │  ├─ repository             reactive Mongo 저장·중복 식별·이력 검색
│  │  ├─ service                인증/권한·내용 검증·멱등 저장·커서·로컬 전파
│  │  ├─ kafka                  접수 이벤트/저장 소비/완료 이벤트 전파
│  │  └─ enums                  공용·게임방 및 게임 종류
│  └─ presence                  접속자 집계·목록 영역
└─ global
   ├─ config                    환경·WebFlux 보안·MongoDB·Kafka·기동 검증
   ├─ exception                 back과 같은 ProblemDetail + code 응답
   ├─ security                  내부 principal·Origin 필터·오류 응답
   ├─ support                   비동기 Kafka 상태 확인
   └─ validation                공통 검증기 영역
```

DTO는 record 대신 builder를 사용한다. Servlet 필터와 동기 MongoDB·Redis 템플릿은 사용하지 않는다. 응답은 back처럼 개별 DTO를 직접 반환하고 오류만 ProblemDetail 형식을 사용한다.

## 로컬 실행

Docker Desktop에서 다음 서비스가 실행 중이어야 한다.

| 서비스 | 로컬 주소 | 용도 |
| --- | --- | --- |
| mongoDB | 127.0.0.1:27017 | playground DB, chat_messages 컬렉션 |
| local-redis | 127.0.0.1:6379 | back과 공유할 세션, 향후 chat: 접속 상태 |
| kafka | 127.0.0.1:9092 | 채팅 처리·전파 |

기존 MongoDB·Redis 데이터는 삭제하거나 초기화하지 않는다. 기동 시 Redis에는 PING만 보내고, 실제 인증 요청에서만 TTL이 있는 chat:auth: 키를 사용한다. 기존 로그인 키는 조회만 한다.

현재 PC에는 Git에서 제외한 src/main/resources/application-local.properties를 준비했다. 다음 명령으로 실행한다.

```powershell
.\gradlew.bat bootRun --args='--spring.profiles.active=local'
```

다른 PC에서는 application-local.properties.example을 application-local.properties로 복사하고 CHAT_MONGODB_URI에 실제 사용자·비밀번호·DB·authSource가 포함된 접속 URI를 공급한다.

```powershell
$env:CHAT_MONGODB_URI='mongodb://사용자:URL인코딩한비밀번호@127.0.0.1:27017/playground?authSource=admin'
.\gradlew.bat bootRun --args='--spring.profiles.active=local'
```

필요하면 CHAT_PORT, CHAT_REDIS_HOST, CHAT_REDIS_PORT, CHAT_REDIS_DATABASE, CHAT_REDIS_USERNAME, CHAT_REDIS_PASSWORD, CHAT_KAFKA_BOOTSTRAP_SERVERS를 덮어쓴다. 로컬 기본 HTTP 주소는 http://127.0.0.1:9082다.

MongoDB 설정은 Boot 4의 **spring.mongodb.uri**를 사용한다. URI의 비밀번호 예약 문자는 URL 인코딩한다. Redis DB 번호는 back과 동일하게 맞추고 향후 채팅 키에는 chat: 접두사를 사용한다.

## 환경과 배포 파일

back처럼 application-local.properties / application-dev.properties / application-prod.properties 세 개의 독립 설정을 사용한다. 공통 application.properties 없이 local, dev, prod 중 하나만 명시적으로 활성화해야 한다. 미선택·혼합·미지원 프로필은 시작을 거절한다.

- local: Windows IDE 또는 bootRun, 127.0.0.1 바인딩. 실제 로컬 설정은 Git과 배포 JAR에서 제외한다.
- dev: Docker 내부망 HTTP. MongoDB·Redis·Kafka 주소는 환경 변수로 공급한다.
- prod: HTTPS 및 Kafka·Redis TLS 기본값. MongoDB URI에도 해당 환경의 인증·TLS 옵션을 지정한다. 일회용 증명·브라우저 키로 접속을 인증하며 프록시 배포는 후속 단계에서 추가한다.

dev/prod의 필수 환경 변수는 CHAT_MONGODB_URI, CHAT_REDIS_HOST, CHAT_REDIS_DATABASE, CHAT_KAFKA_BOOTSTRAP_SERVERS다. Redis 포트·계정은 필요에 따라 추가한다. prod는 CHAT_TLS_CERTIFICATE, CHAT_TLS_PRIVATE_KEY, CHAT_KAFKA_SASL_JAAS_CONFIG, CHAT_KAFKA_REPLICATION_FACTOR, CHAT_KAFKA_MIN_IN_SYNC_REPLICAS도 필요하다. 인증서 값에는 file:/... 형식의 리소스 경로를 사용할 수 있다.

Kafka 인증 방식은 CHAT_KAFKA_SECURITY_PROTOCOL과 CHAT_KAFKA_SASL_MECHANISM으로 설정한다. 사설 CA 등 추가 드라이버 설정은 해당 환경의 Spring 설정으로 공급한다. 현재 운영 배포 스크립트에는 chat을 추가하지 않았다.

```powershell
.\gradlew.bat bootJar
```

배포 JAR에는 local 설정과 .properties.example을 포함하지 않는다. 현재 로컬 Docker Kafka는 Windows 호스트 접속용 127.0.0.1:9092를 광고한다. 앱을 컨테이너에 넣는 배포에서는 Kafka의 advertised.listeners도 앱에서 접근 가능한 주소로 별도 구성해야 한다.

## 기동과 상태 확인

back의 Redis 기동 검증 방식을 reactive 연결에 맞춰 확장했다. 웹 서버가 요청을 받기 전에 MongoDB PING, Redis PING, Kafka describeCluster를 검사한다. 기동 스레드에서만 제한 시간 동안 기다리고 요청 처리나 WebSocket 이벤트 루프에서는 block / Future.get / join을 사용하지 않는다.

기동 실패 시 연결 오류가 난 서비스를 안내하고 종료한다. KafkaAdmin도 fail-fast로 동작하므로 토픽을 준비하지 못하면 시작되지 않는다.

| 경로 | 정상 응답 | 검사 대상 |
| --- | --- | --- |
| GET /actuator/health | 200, status: UP | 전체 상태 |
| GET /actuator/health/liveness | 200, status: UP | 애플리케이션 생존 상태 |
| GET /actuator/health/readiness | 200, status: UP | 준비 상태 + MongoDB·Redis·Kafka |

상태 응답에는 연결 정보나 상세 오류를 노출하지 않는다. 업무 HTTP API와 다른 Actuator 경로는 401로 차단한다. /ws/chat은 Origin 검사 후 업그레이드하되 첫 AUTH 프레임의 일회용 증명과 브라우저 키를 검증해야 인증이 완료된다. 기본 로그인 화면·Basic 인증·개발용 자동 계정·서버 세션은 사용하지 않는다.

기존 로그인과의 연결, 환경 설정, 프레임·종료 코드·Redis 키 계약은 [chat-authentication.md](docs/chat-authentication.md)를 따른다. 현재 PC에서 back과 chat을 재시작하면 새 인증 경로가 반영된다.

## Kafka 토픽

1단계는 토픽만 준비한다. 발행자와 소비자는 아직 만들지 않았다.

| 토픽 | 용도 |
| --- | --- |
| playground.chat.messages.v1 | 수신 메시지의 영속 처리 |
| playground.chat.broadcasts.v1 | MongoDB 저장이 완료된 메시지를 각 chat 서버로 전파 |

초기값은 토픽당 파티션 3개, 보관 기간 7일, 레코드 최대 크기 16KiB다. 로컬 broker는 1개이므로 복제 수와 최소 동기 복제 수는 모두 1이다. 프로덕션은 실제 broker 수에 맞춰 두 값을 명시한다.

수신 토픽과 전파 토픽 이름은 달라야 하고 최소 동기 복제 수는 복제 수를 넘을 수 없다. 토픽은 명시적으로 생성하며 기존 토픽의 보관 기간 등 설정은 자동 변경하지 않는다. 파티션 수를 올리면 KafkaAdmin이 기존 토픽의 파티션을 증가시킬 수 있다. 대화 순서에 영향을 주므로 운영에서 파티션 수 변경은 별도 계획으로 수행한다.

채팅 대상·보안·이력·접속자 계약은 [chat-protocol.md](docs/chat-protocol.md)에 정리했다.

## 3단계: 메시지 저장과 과거 이력

MongoDB `chat_messages` 컬렉션에 인증된 사용자 ID, 닉네임·아바타 스냅샷, 본문, 요청 UUID, UTC 작성 시각을 저장한다. `GLOBAL`은 `global` 단일 채널이고, 게임 방은 추후 `game:gomoku:room:{roomId}` 형식을 사용한다. 클라이언트가 전송한 사용자·대화 키·작성 시각은 신뢰하지 않는다.

- `ChatMessageService.save(principal, destination, clientMessageId, content)`: 로그인 세션을 재검증하고 권한 확인 후 저장한다. 본문은 양끝 공백 제거 후 1~1000 유니코드 코드 포인트, UTF-8 3500바이트 이하이며 제어 문자와 잘못된 서로게이트를 거부한다. 같은 발신자가 같은 대화에 같은 UUID와 동일 본문으로 재시도하면 첫 저장 결과를 반환한다. 동일 UUID를 다른 본문으로 재사용하면 `CHAT_MESSAGE_CONFLICT`다.
- `ChatMessageService.history(principal, destination, before, requestedSize)`: 기본 30개, 최대 50개. 최신 페이지를 가져오되 응답 `messages`는 과거 → 최신순이며, 이전 페이지가 있으면 `nextCursor`를 사용한다. 잘못된 커서·크기는 명시적으로 거부한다.
- 기동 시 `chat_history_v1 (conversationKey, createdAt DESC, _id DESC)`와 `chat_idempotency_v1 UNIQUE (conversationKey, authorId, clientMessageId)` 인덱스를 준비한다. 기존 컬렉션과 문서는 삭제하지 않으며 인덱스 충돌은 조용히 무시하지 않는다.
- WebSocket 인증 완료 이후에만 이력을 조회할 수 있다. 일반 HTTP 채팅 API는 보안 설정상 여전히 열려 있지 않다. 게임 방 메시지는 **8단계 참가 권한 확인 전까지** `CHAT_ROOM_ACCESS_DENIED`로 차단한다.

자세한 데이터 계약과 경계 조건은 [stage-3-message-storage.md](docs/stage-3-message-storage.md)를 참고한다.

## 4단계: Kafka 실시간 처리

`/ws/chat`에서 기존 `AUTH` 인증을 마친 연결만 `CHAT_SEND`, `CHAT_HISTORY`, `PING`을 사용한다. 홈 채팅은 `scope: "GLOBAL"` 한 채널만 지원한다. 송신자·작성 시각·DB ID는 서버가 확정하고, `GAME_ROOM`은 참가자 권한 서비스 구현 이전까지 거부한다.

- `CHAT_SEND` → 1차 인증/내용 검증 → 메시지 토픽에 broker 접수 → `CHAT_ACCEPTED` 응답 → Kafka 저장 소비자가 MongoDB 멱등 저장 → 전파 토픽에 기록 → 각 chat 서버에서 `CHAT_MESSAGE` 송신.
- `CHAT_ACCEPTED`는 **저장 완료 확인이 아니다**. 저장 완료는 서버가 `CHAT_MESSAGE`에 반환한 `id`로 확인한다. Kafka 적어도 한 번(at-least-once) 재처리로 같은 ID가 여러 번 전파될 수 있으니 클라이언트는 `message.id`로 중복을 제거한다.
- 저장 소비 그룹은 `playground-chat-store-v1` 하나를 공유하며, 완료 전파 소비 그룹은 인스턴스마다 달라 현재 연결이 붙은 모든 서버가 수신한다. 완료 전파는 서비스 재시작 때 재생하지 않는다. 놓친 데이터는 `CHAT_HISTORY`로 복구한다.
- MongoDB 저장 완료 후 Kafka 전파 실패 시 동일 offset을 재처리하며 Unique Index로 중복 저장을 막는다. 단, 별도 영속 outbox가 없으므로 broker 접수 전 연결 오류 및 영구적인 Kafka 장애 상황은 송신 UUID를 유지해 재시도해야 한다.
- 수신 프레임 상한은 4096바이트다. 사용자별 연결에서 `PING`은 최소 1초, `CHAT_SEND`는 최소 200ms, 이력 조회는 최소 500ms 간격이다. 90초간 수신이 없으면 연결이 종료되므로 화면 연동 시 주기적으로 PING을 전송한다.
- 느린 수신 연결에는 128개 메시지 버퍼를 적용하며 초과 시 서버가 1013 코드로 해당 연결을 종료한다. 프론트엔드는 재접속 후 이력을 다시 조회해야 한다.

요청·응답의 전체 JSON 예시와 실패 처리 기준은 [stage-4-protocol.md](docs/stage-4-protocol.md)에 있다. 배포 구성은 채팅 기능 완성 후 별도로 작업한다.

## 검증 및 다음 단계

첨부된 3단계 소스에 저장·이력·권한·인덱스 코드가 들어 있는 것을 정적 검토했다. 실행 환경에 Java 25와 MongoDB·Redis·Kafka 인스턴스가 없어 이번 4단계의 실제 Java 빌드·통합 테스트는 수행하지 않았다. 실행 검증은 개발자 Java 25 환경에서 수행해야 한다. 임시 테스트 코드는 추가하지 않았다.

다음 5단계는 Redis 연결별 접속자 관리, 여러 탭 사용자 중복 집계 및 비정상 종료 정리다. 6단계에서 Next.js 채팅 UI·접속자 수·목록을 실데이터에 연결한다. 홈의 기존 채널 선택 UI는 이번 수정 파일에서 제거했다.

참고: [WebFlux WebSocket](https://docs.spring.io/spring-framework/reference/web/webflux-websocket.html), [Spring Kafka](https://docs.spring.io/spring-kafka/reference/).
