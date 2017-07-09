FROM openjdk:8-jre-alpine

ADD target/prometheus-jdbc-exporter-1.0-SNAPSHOT-jar-with-dependencies.jar prometheus-jdbc-exporter.jar

#cd oRUN mkdir lib

EXPOSE 5555

CMD java -Djava.security.egd=file:///dev/urandom -cp prometheus-jdbc-exporter.jar:lib/* no.sysco.middleware.metrics.prometheus.jdbc.WebServer 0.0.0.0:5555 ./config.yml
