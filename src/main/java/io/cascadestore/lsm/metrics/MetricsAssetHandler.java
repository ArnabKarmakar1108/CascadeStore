package io.cascadestore.lsm.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Serves a classpath asset (e.g. dashboard icon) from the metrics package. */
final class MetricsAssetHandler implements HttpHandler {

  private final byte[] content;
  private final String contentType;

  MetricsAssetHandler(String resourceName, String contentType) throws IOException {
    try (InputStream input = MetricsDashboardHandler.class.getResourceAsStream(resourceName)) {
      if (input == null) {
        throw new IOException("Missing metrics resource: " + resourceName);
      }
      this.content = input.readAllBytes();
    }
    this.contentType = contentType;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1);
      return;
    }

    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.getResponseHeaders().set("Cache-Control", "public, max-age=3600");
    exchange.sendResponseHeaders(200, content.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(content);
    }
  }
}
