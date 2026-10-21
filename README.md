# Playground Chat

Spring Boot 4.1.1·Java 25·WebFlux 기반의 독립 채팅 서비스다. MongoDB에는 대화 이력을 저장하고 Kafka로 메시지를 처리·전파하며 Redis는 접속 상태와 기존 로그인 세션 연동에 사용한다.

현재는 **2단계: 기존 로그인과 채팅 접속 인증 연결**까지 구현했다. 메시지 저장·송수신, 접속자 목록과 홈 화면 연동은 이후 단계에서 추가한다.

## 프로젝트 구조

back과 동일한 auth / domain / global 구분과 controller → service → repository 계층을 따른다. 이번 단계에 실제 역할이 있는 클래스만 만들고 나머지 계층은 해당 기능을 구현할 때 추가한다.

```text
com.project.chat
├─ auth                         일회용 접속 증명·브라우저 키·인증 WebSocket
│  ├─ dto                       builder 방식의 인증·제어 프레임
│  ├─ proof                     로그인 키의 ES256 소유 증명
│  ├─ repository                reactive Redis 증명 소비·세션 확인
│  ├─ service                   인증과 현재 세션 검증
│  └─ websocket                 인증·PING/PONG 연결 생명주기
├─ domain
│  ├─ chat
│  │  ├─ dto/internal           builder 방식의 검증된 채팅 대상
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

## 검증

실제 MongoDB·Redis·Kafka를 사용한 상태·보안·공통 오류 응답과 채팅 대상·환경 설정 검증 30개가 통과했다. 연결 장애 시 기동 중단도 서비스별로 확인했다. 사용자 작업 규칙에 따라 검증이 끝난 테스트 소스는 삭제했으며 실행 결과는 [stage-1-verification.md](docs/stage-1-verification.md)에 남겼다.

2단계는 기존 back 보안 체인·실제 Redis·실제 WebSocket과 프론트 인증 모듈을 검증했다. 결과와 범위는 [stage-2-verification.md](docs/stage-2-verification.md)에 남긴다.

## 다음 단계

3단계에서 MongoDB 문서·인덱스·커서 기반 이력을 구현한다. 이어 WebSocket·Kafka 메시지 처리, 접속자 집계, 홈 UI 순서로 진행한다. 홈의 전체 채널 선택 제거와 접속자 버튼·팝업은 프론트 연동 단계에 포함한다.

참고: [Spring Boot NoSQL 설정](https://docs.spring.io/spring-boot/reference/data/nosql.html), [WebFlux WebSocket](https://docs.spring.io/spring-framework/reference/web/webflux-websocket.html), [Spring Kafka 토픽 관리](https://docs.spring.io/spring-kafka/reference/kafka/configuring-topics.html).
