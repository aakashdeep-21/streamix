# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src src
RUN mvn -q -B -DskipTests package

# Runtime stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
# Railway injects PORT; mount a Railway Volume at /data or all logs/offsets vanish on deploy
ENV STREAMIX_DATA_DIR=/data
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
