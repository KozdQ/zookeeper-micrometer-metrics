package vn.baodh.zookeeper.metrics.provider;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusCounter;
import io.micrometer.prometheus.PrometheusDistributionSummary;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.metrics.Gauge;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.Summary;
import org.apache.zookeeper.metrics.SummarySet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicrometerMetricsProvider implements MetricsProvider {

  private final Logger LOG = LoggerFactory.getLogger(MicrometerMetricsProvider.class);
  private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(
      PrometheusConfig.DEFAULT);
  private final MicrometerMetricsServlet servlet = new MicrometerMetricsServlet(registry);
  private final Context rootContext = new Context();
  private final String LABEL = "key";
  private int port = 7000;
  private boolean exportJvmInfo = true;
  private Server server;

  @Override
  public void configure(Properties properties) throws MetricsProviderLifeCycleException {
    LOG.info("Initializing metrics, configuration: {}", properties);
    this.port = Integer.parseInt(properties.getProperty("httpPort", "7000"));
    this.exportJvmInfo = Boolean.parseBoolean(properties.getProperty("exportJvmInfo", "true"));
  }

  @Override
  public void start() throws MetricsProviderLifeCycleException {
    try {
      LOG.info("Starting /metrics HTTP endpoint at port {} exportJvmInfo: {}", port, exportJvmInfo);
      if (exportJvmInfo) {
        DefaultExports.register(registry.getPrometheusRegistry());
      }
      server = new Server(port);
      ServletContextHandler context = new ServletContextHandler();
      context.setContextPath("/");
      server.setHandler(context);
      context.addServlet(new ServletHolder(servlet), "/metrics");
      server.start();
    } catch (Exception err) {
      LOG.error("Cannot start /metrics server", err);
      if (server != null) {
        try {
          server.stop();
        } catch (Exception suppressed) {
          err.addSuppressed(suppressed);
        } finally {
          server = null;
        }
      }
      throw new MetricsProviderLifeCycleException(err);
    }
  }

  @Override
  public MetricsContext getRootContext() {
    return this.rootContext;
  }

  @Override
  public void stop() {
    if (server != null) {
      try {
        server.stop();
      } catch (Exception err) {
        LOG.error("Cannot safely stop Jetty server", err);
      } finally {
        server = null;
      }
    }
  }

  @Override
  public void dump(BiConsumer<String, Object> biConsumer) {
  }

  @Override
  public void resetAllValues() {
  }

  private static class MicrometerMetricsServlet extends HttpServlet {

    private final PrometheusMeterRegistry prometheusMeterRegistry;

    public MicrometerMetricsServlet(PrometheusMeterRegistry registry) {
      this.prometheusMeterRegistry = registry;
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      resp.setStatus(200);
      resp.setContentType("text/plain; version=0.0.4; charset=utf-8");

      try (Writer writer = resp.getWriter()) {
        prometheusMeterRegistry.scrape(writer, resp.getContentType(), parse(req));
        writer.flush();
      }
    }

    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      this.doGet(req, resp);
    }

    private Set<String> parse(HttpServletRequest req) {
      String[] includedParam = req.getParameterValues("name[]");
      return includedParam == null ? Collections.emptySet()
          : new HashSet<>(Arrays.asList(includedParam));
    }
  }

  private class Context implements MetricsContext {

    private final ConcurrentMap<String, MicrometerGaugeWrapper> gauges = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MicrometerCounter> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MicrometerSummary> basicSummaries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MicrometerSummary> summaries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MicrometerLabelledSummary> basicSummarySets = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MicrometerLabelledSummary> summarySets = new ConcurrentHashMap<>();

    @Override
    public MetricsContext getContext(String name) {
      return this;
    }

    @Override
    public Counter getCounter(String name) {
      return counters.computeIfAbsent(name, MicrometerCounter::new);
    }

    @Override
    public void registerGauge(String name, Gauge gauge) {
      Objects.requireNonNull(name);
      gauges.compute(name,
          (id, prev) -> new MicrometerGaugeWrapper(id, gauge, prev != null ? prev.inner : null));
    }

    @Override
    public void unregisterGauge(String name) {
      MicrometerGaugeWrapper existing = gauges.remove(name);
      if (existing != null) {
        existing.unregister();
      }
    }

    @Override
    public Summary getSummary(String name, DetailLevel detailLevel) {
      if (detailLevel == DetailLevel.BASIC) {
        return basicSummaries.computeIfAbsent(name, (n) -> {
          if (summaries.containsKey(n)) {
            throw new IllegalArgumentException("Already registered a non basic summary as " + n);
          }
          return new MicrometerSummary(name, detailLevel);
        });
      } else {
        return summaries.computeIfAbsent(name, (n) -> {
          if (basicSummaries.containsKey(n)) {
            throw new IllegalArgumentException("Already registered a basic summary as " + n);
          }
          return new MicrometerSummary(name, detailLevel);
        });
      }
    }

    @Override
    public SummarySet getSummarySet(String name, DetailLevel detailLevel) {
      if (detailLevel == DetailLevel.BASIC) {
        return basicSummarySets.computeIfAbsent(name, (n) -> {
          if (summarySets.containsKey(n)) {
            throw new IllegalArgumentException(
                "Already registered a non basic summary set as " + n);
          }
          return new MicrometerLabelledSummary(name, detailLevel);
        });
      } else {
        return summarySets.computeIfAbsent(name, (n) -> {
          if (basicSummarySets.containsKey(n)) {
            throw new IllegalArgumentException("Already registered a basic summary set as " + n);
          }
          return new MicrometerLabelledSummary(name, detailLevel);
        });
      }
    }

    private class MicrometerGaugeWrapper {

      private final String name;
      private final io.micrometer.core.instrument.Gauge inner;

      public MicrometerGaugeWrapper(String name, Gauge gauge,
          io.micrometer.core.instrument.Gauge prev) {
        this(name, gauge, prev, Tags.empty());
      }

      public MicrometerGaugeWrapper(String name, Gauge gauge,
          io.micrometer.core.instrument.Gauge prev, String... tags) {
        this(name, gauge, prev, Tags.of(tags));
      }

      public MicrometerGaugeWrapper(String name, Gauge gauge,
          io.micrometer.core.instrument.Gauge prev, Iterable<Tag> tags) {
        this.name = name;
        this.inner = prev != null ? prev
            : io.micrometer.core.instrument.Gauge
                .builder(name, gauge, this::toDoubleFn)
                .strongReference(true)
                .tags(tags)
                .register(registry);
      }

      private double toDoubleFn(Gauge value) {
        return (value != null) ? ((value.get() != null) ? value.get().doubleValue() : 0) : 0;
      }

      public double value() {
        return this.inner.value();
      }

      private void unregister() {
        try {
          registry.remove(inner);
        } catch (IllegalArgumentException err) {
          LOG.error("invalid for metric {}", name, err);
        }
      }

    }

    private class MicrometerSummary implements Summary {

      private final String name;
      private final PrometheusDistributionSummary inner;

      public MicrometerSummary(String name, MetricsContext.DetailLevel level) {
        this(name, level, Tags.empty());
      }

      public MicrometerSummary(String name, MetricsContext.DetailLevel level, String... tags) {
        this(name, level, Tags.of(tags));
      }

      public MicrometerSummary(String name, MetricsContext.DetailLevel level, Iterable<Tag> tags) {
        this(name, level, Tags.of(tags));
      }

      public MicrometerSummary(String name, MetricsContext.DetailLevel level, Tags tags) {
        this.name = name;
        Meter.Id id = new Meter.Id(name, tags, null, null, Meter.Type.DISTRIBUTION_SUMMARY);
        DistributionStatisticConfig config = DistributionStatisticConfig.DEFAULT;
        if (level == MetricsContext.DetailLevel.ADVANCED) {
          this.inner = (PrometheusDistributionSummary) registry.newDistributionSummary(
              id,
              DistributionStatisticConfig.builder()
                  .percentilesHistogram(true)
                  .percentiles(0.5, 0.9, 0.95)
                  .build().merge(config),
              1.0);
        } else {
          this.inner = (PrometheusDistributionSummary) registry.newDistributionSummary(
              id,
              DistributionStatisticConfig.builder()
                  .percentilesHistogram(true)
                  .percentiles(0.5)
                  .build().merge(config),
              1.0);
        }
      }

      @Override
      public void add(long delta) {
        try {
          inner.record(delta);
        } catch (IllegalArgumentException err) {
          LOG.error("invalid delta {} for metric {}", delta, name, err);
        }
      }
    }

    private class MicrometerLabelledSummary implements SummarySet {

      private final String name;
      private final MetricsContext.DetailLevel level;
      private final ConcurrentMap<String, MicrometerSummary> inners;

      public MicrometerLabelledSummary(String name, MetricsContext.DetailLevel level) {
        this.name = name;
        this.level = level;
        this.inners = new ConcurrentHashMap<>();
      }

      @Override
      public void add(String key, long delta) {
        try {
          this.inners.computeIfAbsent(key, (k) -> new MicrometerSummary(name, level, LABEL, key))
              .add(delta);
        } catch (IllegalArgumentException err) {
          LOG.error("invalid value {} for metric {} with key {}", delta, name, key, err);
        }
      }

    }

    private class MicrometerCounter implements Counter {

      private final String name;
      private final PrometheusCounter inner;

      public MicrometerCounter(String name) {
        this(name, Tags.empty());
      }

      public MicrometerCounter(String name, String... tags) {
        this(name, Tags.of(tags));
      }

      public MicrometerCounter(String name, Iterable<Tag> tags) {
        this.name = name;
        this.inner = (PrometheusCounter) registry.counter(name, tags);
      }

      @Override
      public void add(long delta) {
        try {
          inner.increment(delta);
        } catch (IllegalArgumentException err) {
          LOG.error("invalid delta {} for metric {}", delta, name, err);
        }
      }

      @Override
      public long get() {
        return (long) inner.count();
      }
    }
  }
}
