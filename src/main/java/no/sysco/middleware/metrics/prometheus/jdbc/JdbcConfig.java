package no.sysco.middleware.metrics.prometheus.jdbc;

import io.prometheus.client.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 *
 */
class JdbcConfig {
  private static final Logger LOGGER = Logger.getLogger(JdbcConfig.class.getName());

  private List<JdbcJob> jobs = new ArrayList<>();
  private long lastUpdate = 0L;

  JdbcConfig(Map<String, Object> yamlConfig, long lastUpdate) {
    this(yamlConfig);
    this.lastUpdate = lastUpdate;
  }

  JdbcConfig(Map<String, Object> yamlConfig) {
    if (yamlConfig == null) {  // Yaml config empty, set config to empty map.
      yamlConfig = new HashMap<>();
      LOGGER.warning("JDBC Config file is empty.");
    }

    Map<String, String> queries = new TreeMap<>();

    if (yamlConfig.containsKey("queries")) {
      TreeMap<String, Object> labels =
          new TreeMap<>((Map<String, Object>) yamlConfig.get("queries"));
      for (Map.Entry<String, Object> entry : labels.entrySet()) {
        queries.put(entry.getKey(), (String) entry.getValue());
      }
    }

    if (yamlConfig.containsKey("jobs")) {
      final List<Map<String, Object>> jobList =
          Optional.ofNullable((List<Map<String, Object>>) yamlConfig.get("jobs"))
              .orElseThrow(() ->
                  new IllegalArgumentException("JDBC Config file does not have `jobs` defined. " +
                      "It will not collect any metric samples."));

      for (Map<String, Object> jobObject : jobList) {
        JdbcJob job = new JdbcJob();
        jobs.add(job);

        if (jobObject.containsKey("name")) {
          job.name = (String) jobObject.get("name");
        } else {
          throw new IllegalArgumentException("JDBC Job does not have a `name` defined. " +
              "This value is required to execute collector.");
        }

        if (jobObject.containsKey("connections")) {
          final List<Map<String, Object>> connections =
              Optional.ofNullable((List<Map<String, Object>>) jobObject.get("connections"))
                  .orElseThrow(() ->
                      new IllegalArgumentException("JDBC Job does not have `connections` defined. " +
                          "This value is required to execute collector."));

          for (Map<String, Object> connObject : connections) {
            JdbcConnection connection = new JdbcConnection();
            job.connections.add(connection);

            if (connObject.containsKey("url")) {
              connection.url = (String) connObject.get("url");
            } else {
              throw new IllegalArgumentException("JDBC Connection `url` is not defined. " +
                  "This value is required to execute collector.");
            }

            if (connObject.containsKey("username")) {
              connection.username = (String) connObject.get("username");
            } else {
              throw new IllegalArgumentException("JDBC Connection `username` is not defined. " +
                  "This value is required to execute collector.");
            }

            if (connObject.containsKey("password")) {
              connection.password = (String) connObject.get("password");
            } else {
              throw new IllegalArgumentException("JDBC Connection `password` is not defined. " +
                  "This value is required to execute collector.");
            }
          }
        } else {
          throw new IllegalArgumentException("JDBC Job does not have a `connections` defined. " +
              "This value is required to execute collector.");
        }

        if (jobObject.containsKey("queries")) {
          final List<Map<String, Object>> queriesList =
              Optional.ofNullable((List<Map<String, Object>>) jobObject.get("queries"))
                  .orElseThrow(() ->
                      new IllegalArgumentException("JDBC Job does not have `queries` defined. " +
                          "This value is required to execute collector."));

          for (Map<String, Object> queryObject : queriesList) {
            Query query = new Query();
            job.queries.add(query);

            if (queryObject.containsKey("name")) {
              query.name = (String) queryObject.get("name");
            } else {
              throw new IllegalArgumentException("JDBC Query does not have a `name` defined. " +
                  "This value is required to execute collector.");
            }

            if (queryObject.containsKey("help")) {
              query.help = (String) queryObject.get("help");
            }

            if (queryObject.containsKey("labels")) {
              final List<Object> labels =
                  Optional.ofNullable((List<Object>) queryObject.get("labels"))
                      .orElse(new ArrayList<>());

              for (Object label : labels) {
                query.labels.add((String) label);
              }
            }

            if (queryObject.containsKey("values")) {
              final List<Object> values =
                  Optional.ofNullable((List<Object>) queryObject.get("values"))
                      .orElseThrow(() ->
                          new IllegalArgumentException("JDBC Query does not have `values` defined. " +
                              "This value is required to execute collector."));

              for (Object value : values) {
                query.values.add((String) value);
              }
            } else {
              throw new IllegalArgumentException("JDBC Query does not have `values` defined. " +
                  "This value is required to execute collector.");
            }

            if (queryObject.containsKey("query") && queryObject.containsKey("query_ref")) {
              throw new IllegalArgumentException("JDBC Query cannot have a `query` value and a `query_ref` at the same time.");
            }

            if (queryObject.containsKey("query")) {
              query.query = (String) queryObject.get("query");
            } else if (queryObject.containsKey("query_ref")) {
              query.queryRef = (String) queryObject.get("query_ref");
              if (queries.containsKey(query.queryRef)) {
                query.query = queries.get(query.queryRef);
              } else {
                throw new IllegalArgumentException("JDBC Query Reference does not exist as part of the JDBC Queries.");
              }
            } else {
              throw new IllegalArgumentException("JDBC Query must have a `query` value OR a `query_ref` defined.");
            }
          }
        } else {
          throw new IllegalArgumentException("JDBC Job does not have `queries` defined. " +
              "This value is required to execute collector.");
        }
      }
    } else {
      throw new IllegalArgumentException("JDBC Config file does not have jobs defined. " +
          "It will not collect any metric samples.");
    }


  }

  List<Collector.MetricFamilySamples> runJobs() {
    return
        jobs.stream()
            .flatMap(job -> runJob(job).stream())
            .collect(toList());
  }

  private List<Collector.MetricFamilySamples> runJob(JdbcJob job) {
    LOGGER.log(Level.INFO, "Running JDBC job: " + job.name);

    double error = 0;
    List<Collector.MetricFamilySamples> mfsList = new ArrayList<>();
    List<Connection> connections = new ArrayList<>();
    long start = System.nanoTime();

    try {
      List<Collector.MetricFamilySamples> mfsListFromJobs =
          job.connections
              .stream()
              .flatMap(connection -> {
                try {
                  LOGGER.info(String.format("JDBC Connection URL: %s", connection.url));

                  final Connection conn =
                      DriverManager.getConnection(
                          connection.url, connection.username, connection.password);

                  connections.add(conn);

                  return
                      job.queries
                          .stream()
                          .flatMap(query -> {
                            try {
                              PreparedStatement statement = conn.prepareStatement(query.query);
                              ResultSet rs = statement.executeQuery();

                              return getSamples(job.name, query, rs).stream();
                            } catch (SQLException e) {
                              LOGGER.log(Level.SEVERE, String.format("Error executing query: %s", query.query), e);
                              return Stream.empty();
                            }
                          });
                } catch (SQLException e) {
                  LOGGER.log(Level.SEVERE, "Error connecting to database", e);
                  return Stream.empty();
                }
              })
              .collect(toList());

      mfsList.addAll(mfsListFromJobs);
    } catch (Exception e) {
      error = 1;
    }

    connections.forEach(connection -> {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.log(Level.WARNING, "Error closing connection.", e);
      }
    });

    List<Collector.MetricFamilySamples.Sample> samples = new ArrayList<>();
    samples.add(
        new Collector.MetricFamilySamples.Sample(
            "jdbc_scrape_duration_seconds",
            new ArrayList<>(),
            new ArrayList<>(),
            (System.nanoTime() - start) / 1.0E9));
    mfsList.add(
        new Collector.MetricFamilySamples(
            "jdbc_scrape_duration_seconds",
            Collector.Type.GAUGE,
            "Time this JDBC scrape took, in seconds.",
            samples));

    samples = new ArrayList<>();
    samples.add(
        new Collector.MetricFamilySamples.Sample(
            "jdbc_scrape_error",
            new ArrayList<>(),
            new ArrayList<>(),
            error));
    mfsList.add(
        new Collector.MetricFamilySamples(
            "jdbc_scrape_error",
            Collector.Type.GAUGE,
            "Non-zero if this scrape failed.",
            samples));

    return mfsList;
  }

  private List<Collector.MetricFamilySamples> getSamples(String jobName,
                                                         JdbcConfig.Query query,
                                                         ResultSet rs)
      throws SQLException {
    List<Collector.MetricFamilySamples> samplesList = new ArrayList<>();

    while (rs.next()) {
      List<String> labelValues =
          query.labels
              .stream()
              .map(label -> {
                try {
                  return rs.getString(label);
                } catch (SQLException e) {
                  LOGGER.log(
                      Level.WARNING,
                      String.format("Label %s not found as part of the query result set.", label));
                  return "";
                }
              }).collect(toList());

      List<Collector.MetricFamilySamples.Sample> samples =
          query.values.stream()
              .map(value -> {
                try {
                  return rs.getFloat(value);
                } catch (SQLException e) {
                  LOGGER.log(
                      Level.SEVERE,
                      String.format("Sample value %s not found as part of the query result set.", value),
                      e);
                  return null;
                }
              })
              .map(value -> {
                final String name = String.format("jdbc_%s", query.name);
                return
                    new Collector.MetricFamilySamples.Sample(name, query.labels, labelValues, value);
              })
              .collect(toList());

      samplesList.add(
          new Collector.MetricFamilySamples(jobName, Collector.Type.GAUGE, query.help, samples));
    }

    return samplesList;
  }

  long lastUpdate() {
    return lastUpdate;
  }

  private static class JdbcConnection {
    String url;
    String username;
    String password;
  }

  private static class JdbcJob {
    String name;
    List<JdbcConnection> connections = new ArrayList<>();
    List<Query> queries = new ArrayList<>();
  }

  private static class Query {
    String name;
    String help;
    List<String> labels = new ArrayList<>();
    List<String> values = new ArrayList<>();
    String query;
    String queryRef;
  }
}
