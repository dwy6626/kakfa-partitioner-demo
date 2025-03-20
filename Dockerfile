# Stage 1: Build the application using Maven
FROM maven:3.8.6-openjdk-11 AS build
WORKDIR /app
COPY pom.xml ./
COPY src ./src
RUN mvn package

# Stage 2: Run the application using a lightweight JRE image
FROM openjdk:11-jre
WORKDIR /app
COPY --from=build /app/target/kafka-demo-1.0-SNAPSHOT.jar kafka-demo.jar
CMD ["java", "-jar", "kafka-demo.jar"]
