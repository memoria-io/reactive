package io.memoria.reactive.web.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.logging.Log4j2Metrics;
import io.micrometer.core.instrument.binder.system.DiskSpaceMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PrometheusConfiguration {
  private final List<Tag> tagList;
  private final PrometheusMeterRegistry registry;

  public PrometheusConfiguration(String appName, String version) {
    this(appName, version, new ArrayList<>());
  }

  public PrometheusConfiguration(String appName, String version, List<Tag> tagList) {
    this.tagList = tagList;
    tagList.add(Tag.of("APPLICATION_NAME", appName));
    tagList.add(Tag.of("APPLICATION_VERSION", version));
    this.registry = createRegistry();
  }

  public PrometheusMeterRegistry getRegistry() {
    return registry;
  }

  public List<Tag> getTags() {
    return tagList;
  }

  private PrometheusMeterRegistry createRegistry() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.config().commonTags(tagList);
    includeLog4j2Metrics(registry);
    includeThreadMetrics(registry);
    includeGCMetrics(registry);
    includeMemoryMetrics(registry);
    includeDiskSpaceMetrics(registry);
    includeProcessorMetrics(registry);
    includeUptimeMetrics(registry);
    return registry;
  }

  @SuppressWarnings("resource")
  private static void includeLog4j2Metrics(MeterRegistry registry) {
    new Log4j2Metrics().bindTo(registry);
  }

  private static void includeThreadMetrics(MeterRegistry registry) {
    new ClassLoaderMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
  }

  @SuppressWarnings({"java:S2095", "resource"})
  // Do not change JvmGcMetrics to try-with-resources as the JvmGcMetrics will not be available after (auto-)closing.
  // See https://github.com/micrometer-metrics/micrometer/issues/1492
  private static void includeGCMetrics(MeterRegistry registry) {
    JvmGcMetrics jvmGcMetrics = new JvmGcMetrics();
    jvmGcMetrics.bindTo(registry);
    Runtime.getRuntime().addShutdownHook(new Thread(jvmGcMetrics::close));
  }

  private static void includeMemoryMetrics(MeterRegistry registry) {
    new JvmMemoryMetrics().bindTo(registry);
  }

  private static void includeDiskSpaceMetrics(MeterRegistry registry) {
    new DiskSpaceMetrics(new File("/")).bindTo(registry);
  }

  private static void includeProcessorMetrics(MeterRegistry registry) {
    new ProcessorMetrics().bindTo(registry);
  }

  private static void includeUptimeMetrics(MeterRegistry registry) {
    new UptimeMetrics().bindTo(registry);
  }
}