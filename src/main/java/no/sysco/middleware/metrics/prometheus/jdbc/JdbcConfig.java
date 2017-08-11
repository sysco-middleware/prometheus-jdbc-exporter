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
import java.util.TreeMap;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

/**
 *
 */
class JdbcConfig {
  private static final Logger LOGGER = Logger.getLogger(JdbcConfig.class.getName());

  private List<JdbcJob> jobs = new ArrayList<>();
  private Map<String, String> queries = new TreeMap<>();
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

    if (yamlConfig.containsKey("jobs")) {
      List<Map<String, Object>> jobList = (List<Map<String, Object>>) yamlConfig.get("jobs");
      for (Map<String, Object> jobObject : jobList) {
        JdbcJob job = new JdbcJob();
        jobs.add(job);

        if (jobObject.containsKey("name")) {
          job.name = (String) jobObject.get("name");
        } else {
          LOGGER.severe("JDBC Job does not have a name defined. This value is required to name a metric.");
          //TODO throw exception
        }

        if (jobObject.containsKey("connections")) {
          List<Map<String, Object>> connections = (List<Map<String, Object>>) jobObject.get("connections");
          for (Map<String, Object> connObject : connections) {
            JdbcConnection connection = new JdbcConnection();
            job.connections.add(connection);

            if (connObject.containsKey("url")) {
              connection.url = (String) connObject.get("url");
            }
            if (connObject.containsKey("username")) {
              connection.username = (String) connObject.get("username");
            }
            if (connObject.containsKey("password")) {
              connection.password = (String) connObject.get("password");
            }
          }
        } else {
          LOGGER.severe("JDBC Job does not have a connection defined. This value is required to execute collector.");
          //TODO throw exception
        }

        if (jobObject.containsKey("queries")) {
          List<Map<String, Object>> queries = (List<Map<String, Object>>) jobObject.get("queries");
          for (Map<String, Object> queryObject : queries) {
            Query query = new Query();
            job.queries.add(query);

            if (queryObject.containsKey("name")) {
              query.name = (String) queryObject.get("name");
            }
            if (queryObject.containsKey("help")) {
              query.help = (String) queryObject.get("help");
            }
            if (queryObject.containsKey("labels")) {
              List<Object> labels = (List<Object>) queryObject.get("labels");
              for (Object label : labels) {
                query.labels.add((String) label);
              }
            }
            if (queryObject.containsKey("values")) {
              List<Object> values = (List<Object>) queryObject.get("values");
              for (Object value : values) {
                query.values.add((String) value);
              }
            }
            if (queryObject.containsKey("query")) {
              query.query = (String) queryObject.get("query");
            }
            if (queryObject.containsKey("query_ref")) {
              query.queryRef = (String) queryObject.get("query_ref");
            }
          }
        } else {
          LOGGER.severe("JDBC Job does not have queries defined. This value is required to execute collector.");
          //TODO throw exception
        }
      }
    } else {
      LOGGER.warning("Config file does not have jobs defined. It will not collect any metric samples.");
    }

    if (yamlConfig.containsKey("queries")) {
      TreeMap<String, Object> labels =
          new TreeMap<>((Map<String, Object>) yamlConfig.get("queries"));
      for (Map.Entry<String, Object> entry : labels.entrySet()) {
        queries.put(entry.getKey(), (String) entry.getValue());
      }
    }
  }

  List<Collector.MetricFamilySamples> runJobs(){
    return jobs.stream()
        .flatMap(job -> runJob(job, queries).stream())
        .collect(toList());
  }

  private List<Collector.MetricFamilySamples> runJob(JdbcJob job, Map<String, String> queries) {
    double error = 0;
    List<Collector.MetricFamilySamples> mfsList = new ArrayList<>();
    List<Connection> conns = new ArrayList<>();
    long start = System.nanoTime();
    try {
      List<Collector.MetricFamilySamples> mfsListFromJobs =
          job.connections.stream()
              .flatMap(connection -> {
                try {
                  LOGGER.info(String.format("URL: %s", connection.url));
                  Connection conn =
                      DriverManager.getConnection(
                          connection.url, connection.username, connection.password);
                  conns.add(conn);
                  return
                      job.queries.stream()
                          .flatMap(query -> {
                            if (query.query == null) {
                              String q = queries.get(query.queryRef);
                              if (q != null) {
                                query.query = q;
                              } else {
                                return null;
                              }
                            }

                            try {
                              PreparedStatement statement = conn.prepareStatement(query.query);
                              ResultSet rs = statement.executeQuery();

                              return getSamples(job.name, query, rs).stream();
                            } catch (SQLException e) {
                              e.printStackTrace();
                              return null;
                            }
                          });
                } catch (SQLException e) {
                  e.printStackTrace();
                  return null;
                }
              }).collect(toList());
      mfsList.addAll(mfsListFromJobs);
    } catch (Exception e) {
      error = 1;
    }

    conns.forEach(connection -> {
      try {
        connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
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

  private List<Collector.MetricFamilySamples> getSamples(String jobName, JdbcConfig.Query query, ResultSet rs)
      throws SQLException {
    List<Collector.MetricFamilySamples> samplesList = new ArrayList<>();

    while (rs.next()) {
      List<String> labelValues = query.labels.stream().map(label -> {
        try {
          return rs.getString(label);
        } catch (SQLException e) {
          e.printStackTrace();
          return "";
        }
      }).collect(toList());

      List<Collector.MetricFamilySamples.Sample> samples =
          query.values.stream()
              .map(value -> {
                try {
                  return rs.getFloat(value);
                } catch (SQLException e) {
                  e.printStackTrace();
                  return null;
                }
              })
              .map(value -> {
                final String name = String.format("jdbc_%s", query.name);
                return new Collector.MetricFamilySamples.Sample(name, query.labels, labelValues, value);
              })
              .collect(toList());


      samplesList.add(new Collector.MetricFamilySamples(jobName, Collector.Type.GAUGE, query.help, samples));
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
