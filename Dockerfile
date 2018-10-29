FROM openjdk:8
COPY ./src /service/src/
COPY ./pom.xml /service
COPY ./mvnw /service
COPY ./.mvn /service/.mvn
WORKDIR /service

EXPOSE 8080:8080

CMD ["./mvnw", "jetty:run"]