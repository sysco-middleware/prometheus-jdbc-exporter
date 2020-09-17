#!/usr/bin/env bash
export EXPORTER_URL=0.0.0.0:5555
export CONFIG_PATH=./config.yml
export METRIC_PREFIX=jdbc
export LIBS_PATH=./lib
java -Djava.security.egd=file:///dev/urandom -cp ${LIBS_PATH}/* no.sysco.middleware.metrics.prometheus.jdbc.WebServer ${EXPORTER_URL} ${CONFIG_PATH}
