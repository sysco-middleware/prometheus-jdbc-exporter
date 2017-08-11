package no.sysco.middleware.metrics.prometheus.jdbc;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Optional;

/**
 *
 */
public class JdbcCollectorTest {

  @Test
  public void testReloadConfig() {
    final URL resource = getClass().getClassLoader().getResource("config.yml");
    Optional.ofNullable(resource).map(URL::getFile).map(File::new).ifPresent(config -> {
      try {
        JdbcCollector collector = new JdbcCollector(config);
        if (config.setLastModified(System.currentTimeMillis())) {
          collector.reloadConfig();
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    });

  }
}