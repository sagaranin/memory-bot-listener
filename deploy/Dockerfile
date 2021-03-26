FROM openjdk:8-slim-buster

RUN apt-get -y update
RUN mkdir -p /usr/share/man/man1/
RUN apt-get -y install git maven
RUN git clone https://github.com/sagaranin/memory-bot-listener.git
WORKDIR /memory-bot-listener

RUN mvn clean package
RUN cp target/memory-bot-listener-1.0-SNAPSHOT-jar-with-dependencies.jar /app.jar
RUN rm -rf /memory-bot-listener

RUN apt-get -y remove git maven
RUN apt-get -y autoremove

ENTRYPOINT ["java", "-jar", "/app.jar"]
