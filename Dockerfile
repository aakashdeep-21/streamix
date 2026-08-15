# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY streamix-broker/pom.xml streamix-broker/
COPY streamix-client/pom.xml streamix-client/
RUN mvn -q -B -pl streamix-broker -am dependency:go-offline
COPY streamix-broker/src streamix-broker/src
RUN mvn -q -B -DskipTests -pl streamix-broker -am package

# Runtime stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/streamix-broker/target/*-exec.jar app.jar
# Railway injects PORT; mount a Railway Volume at /data or all logs/offsets vanish on deploy
ENV STREAMIX_DATA_DIR=/data
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
