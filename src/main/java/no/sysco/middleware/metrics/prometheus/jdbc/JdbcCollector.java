package no.sysco.middleware.metrics.prometheus.jdbc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.yaml.snakeyaml.Yaml;

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;

/**
 * Prometheus JDBC Collector
 */
public class JdbcCollector extends Collector implements Collector.Describable {
  private static final Counter configReloadSuccess =
      Counter.build()
          .name("jdbc_config_reload_success_total")
          .help("Number of times configuration have successfully been reloaded.").register();

  private static final Counter configReloadFailure =
      Counter.build()
          .name("jdbc_config_reload_failure_total")
          .help("Number of times configuration have failed to be reloaded.").register();

    private static final Logger LOGGER = Logger.getLogger(JdbcCollector.class.getName());

    private Set<JdbcConfig> config;
    private File configFile;
    private long lastUpdate;

    JdbcCollector(File in) {
        configFile = in;
        loadConfigFromFile(in);
    }

    private void loadConfigFromFile(File in) {
        Set<JdbcConfig> config = new HashSet<>();
        lastUpdate = in.lastModified();
        if (in.isDirectory()) {
            for (File file : in.listFiles()) {
                try (FileReader fr = new FileReader(file)) {
                    config.add(new JdbcConfig((Map<String, Object>) new Yaml().load(fr)));
                } catch (Exception iae) {
                    LOGGER.log(Level.SEVERE, "Skipping " + file.getName() + " due to error: " + iae.getMessage(), iae);
                }
            }
        } else {
            try (FileReader fr = new FileReader(in)) {
                config.add(new JdbcConfig((Map<String, Object>) new Yaml().load(fr)));
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Skipping " + in.getName() + " due to error: " + e.getMessage(), e);
            }
        }

        if (config.size() == 0) {
            throw new IllegalArgumentException("Error reading config files: " + in.getName());
        }

        this.config = config;
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

    @Override
    public List<MetricFamilySamples> collect() {
        if (configFile != null) {
            long mtime = configFile.lastModified();
            if (mtime > lastUpdate) {
                LOGGER.fine("Configuration file changed, reloading...");
                reloadConfig();
            }
        }

        return config.stream()
            .flatMap((JdbcConfig jdbcConfig) -> jdbcConfig.runJobs().stream())
            .collect(Collectors.toList());
    }

    void reloadConfig() {
        try {
            loadConfigFromFile(configFile);
            configReloadSuccess.inc();
        } catch (Exception e) {
            LOGGER.severe("Configuration reload failed: " + e.toString());
            configReloadFailure.inc();
        }
    }
}
