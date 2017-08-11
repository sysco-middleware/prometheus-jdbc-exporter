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

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobConnectionsEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobConnectionUrlEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - username: system" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobConnectionUsernameEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobConnectionPasswordEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobQueriesEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobQueryNameEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        "  - something: jdbc\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobQueryValuesEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        "  - name: jdbc\n" +
        "    values:\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobQueryAndRefEmpty() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        "  - name: jdbc\n" +
        "    values:\n" +
        "    - v1\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobQueryAndRef() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        "  - name: jdbc\n" +
        "    values:\n" +
        "    - v1\n" +
        "    query: abc\n" +
        "    query_ref: abc\n" +
        ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigShouldFailIfJobQueryRefNonExistingQuery() {
    new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        "  - name: jdbc\n" +
        "    values:\n" +
        "    - v1\n" +
        "    query_ref: abc\n" +
        ""));
  }

  @Test
  public void testConfigShouldBuildWithQueryRef() {
    JdbcConfig config = new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        "  - name: jdbc\n" +
        "    values:\n" +
        "    - v1\n" +
        "    query_ref: abc\n" +
        "queries:\n" +
        "  abc: \"select * from a\"\n" +
        ""));
    assertNotNull(config);
  }


  @Test
  public void testConfigShouldBuildWithoutQueryRef() {
    JdbcConfig config = new JdbcConfig((Map<String, Object>) new Yaml().load("---\n" +
        "jobs:\n" +
        "- name: \"global\"\n" +
        "  connections:\n" +
        "  - url: jdbc\n" +
        "    username: sys\n" +
        "    password: sys\n" +
        "  queries:\n" +
        "  - name: jdbc\n" +
        "    values:\n" +
        "    - v1\n" +
        "    query: abc\n" +
        ""));
    assertNotNull(config);
  }

}