package no.sysco.middleware.metrics.prometheus.jdbc;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.Map;

import static org.junit.Assert.*;

/**
 *
 */
public class JdbcConfigTest {

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfEmpty() {
    new JdbcConfig(null);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobsEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobNameEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- something: \"global\"\n"));
  }

  @Test//(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobConnectionsEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n"));
  }
}