package io.cascadestore.lsm.metrics;

import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes CascadeStore metrics:
 *
 * <ul>
 *   <li>{@code /} — browser dashboard (human-readable)
 *   <li>{@code /metrics} — Prometheus scrape format (for scrapers, not humans)
 * </ul>
 */
public final class PrometheusHttpServer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(PrometheusHttpServer.class);

  private final HttpServer httpServer;
  private final int port;

  public PrometheusHttpServer(int port) throws IOException {
    this(port, CollectorRegistry.defaultRegistry);
  }

  public PrometheusHttpServer(int port, CollectorRegistry registry) throws IOException {
    DefaultExports.initialize();

    InetSocketAddress bindAddress = bindAddress(port);
    httpServer = HttpServer.create(bindAddress, 0);
    httpServer.createContext("/metrics", new HTTPServer.HTTPMetricHandler(registry));
    httpServer.createContext("/metrics-icon.png", new MetricsAssetHandler("metrics-icon.png", "image/png"));
    httpServer.createContext("/", new MetricsDashboardHandler());
    httpServer.setExecutor(
        Executors.newCachedThreadPool(
            runnable -> {
              Thread thread = new Thread(runnable, "cascadestore-metrics-http");
              thread.setDaemon(true);
              return thread;
            }));
    httpServer.start();

    this.port = httpServer.getAddress().getPort();
    logger.info(
        "Metrics server listening on port {} (dashboard: /, scrape: /metrics)", this.port);
  }

  public int getPort() {
    return port;
  }

  @Override
  public void close() {
    httpServer.stop(0);
    logger.info("Prometheus metrics server stopped");
  }

  private static InetSocketAddress bindAddress(int port) throws IOException {
    if (port == 0) {
      // Let the OS pick a free port on IPv4 loopback (matches scrape URLs in tests/CI).
      return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }
    return new InetSocketAddress(port);
  }
}
