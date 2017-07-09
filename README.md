# Prometheus JDBC Exporter

Exporter inspired in [sql_exporter](https://github.com/justwatchcom/sql_exporter) and [jmx_exporter](https://github.com/prometheus/jmx_exporter)

It uses JDBC libraries to execute a SQL query that returns a `Float` result and a set of labels.

## Getting Started

A YAML configuration file is required. 

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

## Configuration

This is a list of all possible options:

//TODO

## Examples

Go to the `examples` directory.

## Licence

MIT Licenced.


