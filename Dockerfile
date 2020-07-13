FROM openjdk:8-jre-alpine

COPY target/prometheus-jdbc-exporter-jar-with-dependencies.jar /target/
COPY lib/README.md /lib/README.md

EXPOSE 5555

CMD java -Djava.security.egd=file:///dev/urandom -cp "/target/*:/lib/*" no.sysco.middleware.metrics.prometheus.jdbc.WebServer 0.0.0.0:5555 ./config.yml
