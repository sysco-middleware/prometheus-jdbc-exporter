package no.sysco.middleware.metrics.prometheus.jdbc;

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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

  private JdbcConfig config;
  private File configFile;

  JdbcCollector(File in) throws FileNotFoundException {
    configFile = in;
    config = new JdbcConfig((Map<String, Object>) new Yaml().load(new FileReader(in)));
    config.lastUpdate = configFile.lastModified();
  }

  JdbcCollector(String yamlConfig) {
    config = new JdbcConfig((Map<String, Object>) new Yaml().load(yamlConfig));
  }

  @Override
  public List<MetricFamilySamples> collect() {
    if (configFile != null) {
      long mtime = configFile.lastModified();
      if (mtime > config.lastUpdate) {
        LOGGER.fine("Configuration file changed, reloading...");
        reloadConfig();
      }
    }

    return config.runJobs();
  }

  private void reloadConfig() {
    try (FileReader fr = new FileReader(configFile)) {
      Map<String, Object> newYamlConfig = (Map<String, Object>) new Yaml().load(fr);
      config = new JdbcConfig(newYamlConfig);
      config.lastUpdate = configFile.lastModified();
      configReloadSuccess.inc();
    } catch (Exception e) {
      LOGGER.severe("Configuration reload failed: " + e.toString());
      configReloadFailure.inc();
    }
  }

  @Override
  public List<MetricFamilySamples> describe() {
    List<MetricFamilySamples> sampleFamilies = new ArrayList<>();
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
}
