FROM maven:3.6.3-jdk-11 AS build

COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package -DskipTests

FROM openjdk:11-jre-slim
RUN apt-get update -y
RUN apt-get install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get install python3.9 -y
RUN apt-get install python3-pip -y
RUN apt-get install git -y

RUN git clone https://github.com/CSE-603-Connectors/pmeter.git
RUN rm -rf pmeter/.git
RUN ls -la
RUN cd pmeter; pip install .

COPY --from=build /home/app/target/ods-transfer-service-0.0.1-SNAPSHOT.jar /usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar


EXPOSE 8083
ENTRYPOINT ["java","-jar","/usr/local/lib/ods-transfer-service-0.0.1-SNAPSHOT.jar"]

