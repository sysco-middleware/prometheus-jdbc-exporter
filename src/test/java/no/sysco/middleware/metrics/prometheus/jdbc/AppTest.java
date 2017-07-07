package no.sysco.middleware.metrics.prometheus.jdbc;

import io.prometheus.client.Collector;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import no.sysco.middleware.metrics.prometheus.jdbc.prometheus.jdbc.JdbcCollector;

import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

  public void testApp() {
    JdbcCollector collector = new JdbcCollector("");
    List<Collector.MetricFamilySamples> samples = collector.collect();
    samples.forEach(sample -> System.out.printf(sample.toString()));
  }
}
