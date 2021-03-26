FROM openjdk:8-slim-buster

RUN apt-get -y update
RUN mkdir -p /usr/share/man/man1/
RUN apt-get -y install git maven
RUN git clone https://github.com/sagaranin/wiki-stream.git
WORKDIR /wiki-stream

RUN mvn clean package
RUN cp target/wiki-stream-1.0-SNAPSHOT-jar-with-dependencies.jar /app.jar
RUN rm -rf /wiki-stream

RUN apt-get -y remove git maven
RUN apt-get -y autoremove

ENTRYPOINT ["java", "-jar", "/app.jar"]
