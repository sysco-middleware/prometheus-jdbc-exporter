# Prometheus JDBC Exporter

Exporter inspired in [sql_exporter](https://github.com/justwatchcom/sql_exporter) and [jmx_exporter](https://github.com/prometheus/jmx_exporter)

It uses JDBC libraries to execute a SQL query that returns a `Float` result and a set of labels.

## Getting Started

A YAML configuration file or directory with multiple YAML configuration files is required. 

Here is a sample:

```yaml
jobs:
- name: "global"
  connections:
  - url: 'jdbc:oracle:thin:@db:1521/ORCLPDB1'
    username: 'system'
    password: 'welcome1'
  queries:
  - name: "db_users"
    help: "Database Users"
    values:
      - "count"
    query:  |
            select count(1) count from dba_users
```

This configuration contains a list of Jobs and a list of Queries.

The first important part here is the `connections` object. It has the JDBC URL, username, password.

These are used to create connections to execute the queries defined inside the Job.

This query will create a metric `sql_db_users`, where *sql_* is the exporter prefix.

## Startup

Startup syntax is 
```
java -Djava.security.egd=file:///dev/urandom \ 
    -cp "." \ 
    no.sysco.middleware.metrics.prometheus.jdbc.WebServer \ 
    <[ip:]port> \ 
    <configFileOrDirectory>
```                     

configFileOrDirectory can point to a directory with at least one valid yaml file or directly a valid one yaml file.

## Configuration

This is a list of all possible options:

**1. jobs**

Represents a list of jobs that will be executed by the collector.

Values:

*name*: Name of the `job`. Required.

*connections*: List of connection details. At least one.

*queries*: List of queries to execute. At least one.

```yaml
jobs:
  - name: "job1"
    connections: ...
    queries: ...
```

**1.1. connection**

Represents connection details to connect to a database instance and
execute queries.

Values:

*url*: JDBC URL to connect to database instances. Required.

*username*: database user's name. Required.

*password*: database user's password. Required.

```yaml
connections:
  - url: 'jdbc:oracle:thin:@db:1521/ORCLPDB1'
    username: 'system'
    password: 'welcome1'
```

**1.2. query**

Represents query definition to collect metrics from database.

Values:

*name*: Query name, that will be part of the metric name: `jdbc_<query_name>`. Required.

*help*: Query description, that will be used as metric description also. Optional.

*static_labels*: Set of label names, and their static values. Optional.

*labels*: List of labels, that has to match a column value, that must be `string`. Optional.

*values*: List of values, that has to match a column value, that must be `float`. At least one.

*query*: SQL query to select rows that will represent a metric sample.

*query_ref*: Reference to common queries shared between jobs.

`query` and `query_ref` are mutually exclusive. At least one of those has to be defined.

```yaml
  queries:
  - name: "db_users"
    help: "Database Users"
    values:
      - "count"
    query:  |
            select count(1) count from dba_users
```

or with `query_ref`

```yaml
  queries:
  - name: "db_users"
    help: "Database Users"
    values:
      - "count"
    query_ref: "query1"
```

**2. queries**

Represents common queries that can be referenced from different `jobs`.

```yaml
queries:
  query1: |
    SELECT count(1) "COUNT" FROM users
```

Where `query1` is the key that will be used from `query` definition.

## Override metric prefix

The default `jdbc` prefix to all metrics can be overriden via the env variable METRIC_PREFIX and will prefix
all metrics with `<METRIC_PREFIX>_`.

## Examples

Go to the `examples` directory.

## Licence

MIT Licenced.


