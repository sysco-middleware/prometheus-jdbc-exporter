package no.sysco.middleware.metrics.prometheus.jdbc;

import io.prometheus.client.Collector;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import no.sysco.middleware.metrics.prometheus.jdbc.prometheus.jdbc.JdbcCollector;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

  public void testApp() {
    File file = new File("/home/jeqo/dev/sysco/prometheus-jdbc-exporter/src/test/resources/config.yml");
    try {
      JdbcCollector collector = new JdbcCollector(file);
      List<Collector.MetricFamilySamples> samples = collector.collect();
      samples.forEach(sample -> System.out.printf(sample.toString()));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
