FROM openjdk:8-jre-alpine

ADD target/prometheus-jdbc-exporter.jar lib/

EXPOSE 5555

CMD java -Djava.security.egd=file:///dev/urandom -cp lib/* no.sysco.middleware.metrics.prometheus.jdbc.WebServer 0.0.0.0:5555 ./config.yml
