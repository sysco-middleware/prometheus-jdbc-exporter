package no.sysco.middleware.metrics.prometheus.jdbc;

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
 * Prometheus JDBC Collector
 */
public class JdbcCollector extends Collector implements Collector.Describable {
  private static final Counter configReloadSuccess = Counter.build()
      .name("jdbc_config_reload_success_total")
      .help("Number of times configuration have successfully been reloaded.").register();

  private static final Counter configReloadFailure = Counter.build()
      .name("jdbc_config_reload_failure_total")
      .help("Number of times configuration have failed to be reloaded.").register();

  private static final Logger LOGGER = Logger.getLogger(JdbcCollector.class.getName());

  private Config config;
  private File configFile;

  public JdbcCollector(File in) throws FileNotFoundException {
    configFile = in;
    config = loadConfig((Map<String, Object>) new Yaml().load(new FileReader(in)));
    config.lastUpdate = configFile.lastModified();
  }

  public JdbcCollector(String yamlConfig) {
    config = loadConfig((Map<String, Object>) new Yaml().load(yamlConfig));
  }

  public static void main(String[] args) {
    System.out.println("Hello World!");
  }

  private Config loadConfig(Map<String, Object> yamlConfig) {
    Config cfg = new Config();

    if (yamlConfig == null) {  // Yaml config empty, set config to empty map.
      yamlConfig = new HashMap<>();
    }

    if (yamlConfig.containsKey("jobs")) {
      List<Map<String, Object>> jobs = (List<Map<String, Object>>) yamlConfig.get("jobs");
      for (Map<String, Object> jobObject : jobs) {
        Job job = new Job();
        cfg.jobs.add(job);

        if (jobObject.containsKey("name")) {
          job.name = (String) jobObject.get("name");
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
        }
        //TODO check validations
      }
    }

    if (yamlConfig.containsKey("queries")) {
      TreeMap<String, Object> labels =
          new TreeMap<>((Map<String, Object>) yamlConfig.get("queries"));
      for (Map.Entry<String, Object> entry : labels.entrySet()) {
        cfg.queries.put(entry.getKey(), (String) entry.getValue());
      }
    }

    return cfg;
  }

  public List<MetricFamilySamples> collect() {
    if (configFile != null) {
      long mtime = configFile.lastModified();
      if (mtime > config.lastUpdate) {
        LOGGER.fine("Configuration file changed, reloading...");
        reloadConfig();
      }
    }

    return
        config.jobs.stream()
            .flatMap(job -> runJob(job, config.queries).stream())
            .collect(toList());
  }

  private List<MetricFamilySamples> runJob(Job job, Map<String, String> queries) {
    double error = 0;
    List<MetricFamilySamples> mfsList = new ArrayList<>();
    List<Connection> conns = new ArrayList<>();
    long start = System.nanoTime();
    try {
      List<MetricFamilySamples> mfsListFromJobs =
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
                            if (query.query != null) {
                              try {
                                PreparedStatement statement = conn.prepareStatement(query.query);
                                ResultSet rs = statement.executeQuery();

                                return getSamples(job.name, query, rs).stream();
                              } catch (SQLException e) {
                                e.printStackTrace();
                                return null;
                              }
                            } else {
                              String q = queries.get(query.queryRef);
                              if (q != null) {
                                try {
                                  PreparedStatement statement = conn.prepareStatement(q);
                                  ResultSet rs = statement.executeQuery();

                                  return getSamples(job.name, query, rs).stream();
                                } catch (SQLException e) {
                                  e.printStackTrace();
                                  return null;
                                }
                              } else {
                                return null;
                              }
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
    List<MetricFamilySamples.Sample> samples = new ArrayList<>();
    samples.add(
        new MetricFamilySamples.Sample(
            "jdbc_scrape_duration_seconds",
            new ArrayList<>(),
            new ArrayList<>(),
            (System.nanoTime() - start) / 1.0E9));
    mfsList.add(
        new MetricFamilySamples(
            "jdbc_scrape_duration_seconds",
            Type.GAUGE,
            "Time this JDBC scrape took, in seconds.",
            samples));

    samples = new ArrayList<>();
    samples.add(
        new MetricFamilySamples.Sample(
            "jdbc_scrape_error",
            new ArrayList<>(),
            new ArrayList<>(),
            error));
    mfsList.add(
        new MetricFamilySamples(
            "jdbc_scrape_error",
            Type.GAUGE,
            "Non-zero if this scrape failed.",
            samples));

    return mfsList;
  }

  private List<MetricFamilySamples> getSamples(String jobName, Query query, ResultSet rs)
      throws SQLException {
    List<MetricFamilySamples> samplesList = new ArrayList<>();

    while (rs.next()) {
      List<String> labelValues = query.labels.stream().map(label -> {
        try {
          return rs.getString(label);
        } catch (SQLException e) {
          e.printStackTrace();
          return "";
        }
      }).collect(toList());

      List<MetricFamilySamples.Sample> samples =
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
                final String name = String.format("sql_%s", query.name);
                return new MetricFamilySamples.Sample(name, query.labels, labelValues, value);
              })
              .collect(toList());


      samplesList.add(new MetricFamilySamples(jobName, Type.GAUGE, query.help, samples));
    }
    return samplesList;
  }

  private void reloadConfig() {
    try (FileReader fr = new FileReader(configFile)) {
      Map<String, Object> newYamlConfig = (Map<String, Object>) new Yaml().load(fr);
      config = loadConfig(newYamlConfig);
      config.lastUpdate = configFile.lastModified();
      configReloadSuccess.inc();
    } catch (Exception e) {
      LOGGER.severe("Configuration reload failed: " + e.toString());
      configReloadFailure.inc();
    }
  }

  public List<MetricFamilySamples> describe() {
    List<MetricFamilySamples> sampleFamilies = new ArrayList<MetricFamilySamples>();
    sampleFamilies.add(
        new MetricFamilySamples(
            "jdbc_scrape_duration_seconds",
            Type.GAUGE,
            "Time this JDBC scrape took, in seconds.",
            new ArrayList<>()));
    sampleFamilies.add(
        new MetricFamilySamples(
            "jdbc_scrape_error",
            Type.GAUGE,
            "Non-zero if this scrape failed.",
            new ArrayList<>()));
    return sampleFamilies;
  }

  static class Config {
    List<Job> jobs = new ArrayList<>();
    Map<String, String> queries = new TreeMap<>();
    long lastUpdate = 0L;
  }

  static class JdbcConnection {
    String url;
    String username;
    String password;
  }

  static class Job {
    String name;
    List<JdbcConnection> connections = new ArrayList<>();
    List<Query> queries = new ArrayList<>();
  }

  static class Query {
    String name;
    String help;
    List<String> labels = new ArrayList<>();
    List<String> values = new ArrayList<>();
    String query;
    String queryRef;
  }
}
