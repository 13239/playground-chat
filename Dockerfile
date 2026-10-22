# syntax=docker/dockerfile:1

# back과 동일한 Java 25 환경에서 WebFlux 채팅 서비스의 실행 JAR를 만든다.
FROM eclipse-temurin:25-jdk-noble AS builder
WORKDIR /build
COPY gradle ./gradle
COPY gradlew build.gradle settings.gradle lombok.config ./
# Windows의 Wrapper 줄바꿈도 Linux 빌드에서 그대로 처리한다.
RUN sed -i 's/\r$//' gradlew && chmod +x gradlew
COPY src/main ./src/main
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon bootJar && cp build/libs/*.jar /build/application.jar

FROM eclipse-temurin:25-jre-noble AS runtime
WORKDIR /app
RUN groupadd --gid 10001 playground && useradd --uid 10001 --gid playground --no-create-home playground
COPY --from=builder --chown=10001:10001 /build/application.jar ./application.jar
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70.0 -XX:+ExitOnOutOfMemoryError"
USER 10001:10001
EXPOSE 9082
# MongoDB·Redis·Kafka 연결 정보와 인증 정책은 컨테이너 시작 시 공급한다.
ENTRYPOINT ["java", "-jar", "application.jar"]
