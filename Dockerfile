## Build stage
FROM gradle:8.14.4-jdk21 AS build
WORKDIR /workspace

COPY build.gradle settings.gradle ./
COPY src src

RUN gradle --no-daemon bootJar -x test

## Runtime stage
FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/build/libs/app.jar app.jar

EXPOSE 8082
ENTRYPOINT ["java","-jar","/app/app.jar"]
