package io.cascadestore.lsm.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/** Serves a browser dashboard at {@code /} that visualizes {@code /metrics}. */
final class MetricsDashboardHandler implements HttpHandler {

  private static final String HTML =
      """
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>CascadeStore Metrics</title>
        <style>
          :root {
            color-scheme: dark;
            --bg: #0f1419;
            --panel: #171d26;
            --border: #2a3441;
            --text: #e7ecf3;
            --muted: #8b98a8;
            --accent: #5b9cff;
            --good: #3ecf8e;
            --warn: #f5a524;
          }
          * { box-sizing: border-box; }
          body {
            margin: 0;
            font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.5;
          }
          header {
            padding: 24px 28px 12px;
            border-bottom: 1px solid var(--border);
            background: linear-gradient(180deg, #141b24, var(--bg));
          }
          .title-row {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 6px;
          }
          .title-row img {
            width: 36px;
            height: 36px;
            flex-shrink: 0;
          }
          h1 { margin: 0; font-size: 1.6rem; }
          .sub { color: var(--muted); font-size: 0.95rem; }
          .links { margin-top: 12px; }
          .links a {
            color: var(--accent);
            text-decoration: none;
            margin-right: 16px;
          }
          main { padding: 20px 28px 40px; }
          .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            gap: 16px;
          }
          section {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 16px 18px;
          }
          section h2 {
            margin: 0 0 12px;
            font-size: 0.85rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--muted);
          }
          .metric {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            padding: 8px 0;
            border-top: 1px solid rgba(255,255,255,0.05);
          }
          .metric:first-of-type { border-top: none; padding-top: 0; }
          .name { color: var(--muted); font-size: 0.92rem; }
          .value {
            font-variant-numeric: tabular-nums;
            font-weight: 600;
            text-align: right;
          }
          .value.good { color: var(--good); }
          .value.warn { color: var(--warn); }
          .status {
            margin-top: 16px;
            color: var(--muted);
            font-size: 0.9rem;
          }
          .error { color: #ff6b6b; }
        </style>
      </head>
      <body>
        <header>
          <div class="title-row">
            <img src="/metrics-icon.png" alt="CascadeStore metrics icon" width="36" height="36">
            <h1>CascadeStore Metrics</h1>
          </div>
          <div class="sub">Live view of engine health. Auto-refreshes every 2 seconds.</div>
          <div class="links">
            <a href="/metrics" target="_blank">Raw Prometheus scrape</a>
          </div>
        </header>
        <main>
          <div class="grid" id="sections"></div>
          <div class="status" id="status">Loading…</div>
        </main>
        <script>
          const SECTIONS = {
            "Storage": [
              "cascadestore_memtable_bytes",
              "cascadestore_memtable_entries",
              "cascadestore_immutable_memtables_pending",
              "cascadestore_sstable_count",
              "cascadestore_compaction_pending",
              "cascadestore_compaction_in_progress"
            ],
            "Reads": [
              "cascadestore_read_operations_total",
              "cascadestore_sstable_lookups_total",
              "cascadestore_bloom_probes_total",
              "cascadestore_bloom_negative_total"
            ],
            "Writes & WAL": [
              "cascadestore_user_write_bytes_total",
              "cascadestore_wal_bytes_written_total",
              "cascadestore_sstable_bytes_written_total",
              "cascadestore_flush_total",
              "cascadestore_compaction_total"
            ],
            "Block cache": [
              "cascadestore_block_cache_bytes",
              "cascadestore_block_cache_entries",
              "cascadestore_block_cache_hits_total",
              "cascadestore_block_cache_misses_total"
            ],
            "Latency (count)": [
              "cascadestore_flush_duration_seconds_count",
              "cascadestore_compaction_duration_seconds_count",
              "cascadestore_wal_fsync_duration_seconds_count"
            ]
          };

          function parseMetrics(text) {
            const metrics = new Map();
            for (const line of text.split("\\n")) {
              if (!line || line.startsWith("#")) continue;
              const space = line.indexOf(" ");
              if (space < 0) continue;
              const namePart = line.slice(0, space);
              const value = line.slice(space + 1).trim();
              const brace = namePart.indexOf("{");
              const name = brace >= 0 ? namePart.slice(0, brace) : namePart;
              const labels = brace >= 0 ? namePart.slice(brace) : "";
              const key = name + labels;
              metrics.set(key, value);
            }
            return metrics;
          }

          function formatValue(name, raw) {
            const value = Number(raw);
            if (Number.isNaN(value)) return raw;
            if (name.endsWith("_bytes")) {
              if (value >= 1048576) return (value / 1048576).toFixed(2) + " MB";
              if (value >= 1024) return (value / 1024).toFixed(1) + " KB";
              return value.toFixed(0) + " B";
            }
            if (name.endsWith("_pending") || name.endsWith("_in_progress")) {
              return value >= 1 ? "yes" : "no";
            }
            if (Number.isInteger(value)) return value.toLocaleString();
            return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
          }

          function labelSuffix(key) {
            const start = key.indexOf("{");
            if (start < 0) return "";
            return key.slice(start);
          }

          function prettyName(name) {
            return name
              .replace(/^cascadestore_/, "")
              .replace(/_total$/, "")
              .replace(/_seconds_count$/, " samples")
              .replace(/_/g, " ");
          }

          function render(metrics) {
            const root = document.getElementById("sections");
            root.innerHTML = "";

            for (const [title, names] of Object.entries(SECTIONS)) {
              const section = document.createElement("section");
              section.innerHTML = "<h2>" + title + "</h2>";

              let added = 0;
              for (const baseName of names) {
                const matches = [...metrics.entries()].filter(([k]) => k.startsWith(baseName));
                if (matches.length === 0) {
                  const row = document.createElement("div");
                  row.className = "metric";
                  row.innerHTML =
                    '<span class="name">' + prettyName(baseName) + '</span><span class="value">—</span>';
                  section.appendChild(row);
                  added++;
                  continue;
                }
                for (const [key, raw] of matches) {
                  const row = document.createElement("div");
                  row.className = "metric";
                  const valueClass =
                    baseName.endsWith("_pending") || baseName.endsWith("_in_progress")
                      ? raw === "1.0" || raw === "1"
                        ? "value warn"
                        : "value good"
                      : "value";
                  row.innerHTML =
                    '<span class="name">' +
                    prettyName(baseName) +
                    labelSuffix(key) +
                    '</span><span class="' +
                    valueClass +
                    '">' +
                    formatValue(baseName, raw) +
                    "</span>";
                  section.appendChild(row);
                  added++;
                }
              }

              if (added > 0) root.appendChild(section);
            }
          }

          async function refresh() {
            const status = document.getElementById("status");
            try {
              const response = await fetch("/metrics", { cache: "no-store" });
              if (!response.ok) throw new Error("HTTP " + response.status);
              const text = await response.text();
              render(parseMetrics(text));
              status.textContent = "Updated " + new Date().toLocaleTimeString();
              status.className = "status";
            } catch (error) {
              status.textContent = "Failed to load metrics: " + error.message;
              status.className = "status error";
            }
          }

          refresh();
          setInterval(refresh, 2000);
        </script>
      </body>
      </html>
      """;

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1);
      return;
    }

    byte[] body = HTML.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(body);
    }
  }
}
