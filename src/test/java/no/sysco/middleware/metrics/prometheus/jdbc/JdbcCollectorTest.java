package no.sysco.middleware.metrics.prometheus.jdbc;

import io.prometheus.client.CollectorRegistry;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Optional;

/**
 *
 */
public class JdbcCollectorTest {

    @Before
    public void setUp() {
        CollectorRegistry.defaultRegistry.clear();
    }

    @Test
    public void testReloadConfig() {
        final URL resource = getClass().getClassLoader().getResource("config.yml");
        Optional.ofNullable(resource).map(URL::getFile).map(File::new).ifPresent(config -> {
            JdbcCollector collector = new JdbcCollector(config);
            if (config.setLastModified(System.currentTimeMillis())) {
                collector.reloadConfig();
            }
        });

    }

    @Test
    public void testMultiFileConfig() {
        final URL resource = getClass().getClassLoader().getResource("multipleconfigs");
        Optional.ofNullable(resource).map(URL::getFile).map(File::new).ifPresent(config -> {
            JdbcCollector collector = new JdbcCollector(config);
            if (config.setLastModified(System.currentTimeMillis())) {
                collector.reloadConfig();
            }
        });

    }
}