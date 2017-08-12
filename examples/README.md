# `Prometheus-JDBC-Exporter` Examples

There is a couple of examples with 2 databases: MySQL and Postgres.

As requisites this examples you will require Docker tools: Docker Server and
Docker Compose.

To run this examples, access one directory (`mysql` or `postgres`) and
run `docker-compose up -d` command to start containers.

After `10s` you will see metric samples on `http://(docker-host ip):9090`.

This examples the steps required to use this exporter tool:

1. Download and put your JDBC JAR file, specific to a Database, under
the same directory as the exporter JAR is. This is required to run
the start command that uses the `target` directory to store this values.

2. Define a `config.yml` file with the SQL queries and connection details
that will be used to collect information from a database.

3. Define a target on the `prometheus.yml` file, that will point to the
exporter endpoint.
