package io.cascadestore.lsm.manifest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Atomic read/write for {@code MANIFEST} in the data directory. */
public final class ManifestStore {

  private static final Logger logger = LoggerFactory.getLogger(ManifestStore.class);

  public static final String MANIFEST_FILE = "MANIFEST";
  public static final String MANIFEST_TMP_FILE = "MANIFEST.tmp";

  private final Path dataDirectory;

  public ManifestStore(String dataDirectory) {
    if (dataDirectory == null || dataDirectory.isEmpty()) {
      throw new IllegalArgumentException("dataDirectory must not be null or empty");
    }
    this.dataDirectory = Path.of(dataDirectory);
  }

  public Optional<Manifest> load() throws IOException {
    Path manifestPath = dataDirectory.resolve(MANIFEST_FILE);
    if (!Files.exists(manifestPath)) {
      return Optional.empty();
    }

    List<String> lines = Files.readAllLines(manifestPath, StandardCharsets.UTF_8);
    int version = Manifest.CURRENT_VERSION;
    long flushedWalSequence = -1;
    List<Manifest.SSTableRef> sstables = new ArrayList<>();

    for (String rawLine : lines) {
      String line = rawLine.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }

      int separator = line.indexOf('=');
      if (separator <= 0) {
        throw new IOException("Invalid MANIFEST line: " + rawLine);
      }

      String key = line.substring(0, separator).trim();
      String value = line.substring(separator + 1).trim();

      switch (key) {
        case "version" -> version = Integer.parseInt(value);
        case "flushed_wal_sequence" -> flushedWalSequence = Long.parseLong(value);
        case "sstable" -> sstables.add(parseSSTableRef(value));
        default -> logger.warn("Ignoring unknown MANIFEST key: {}", key);
      }
    }

    if (version != Manifest.CURRENT_VERSION) {
      throw new IOException("Unsupported MANIFEST version: " + version);
    }

    return Optional.of(new Manifest(version, flushedWalSequence, sstables));
  }

  public void save(Manifest manifest) throws IOException {
    if (manifest == null) {
      throw new IllegalArgumentException("manifest must not be null");
    }

    Files.createDirectories(dataDirectory);

    StringBuilder builder = new StringBuilder();
    builder.append("version=").append(manifest.version()).append('\n');
    builder.append("flushed_wal_sequence=").append(manifest.flushedWalSequence()).append('\n');
    for (Manifest.SSTableRef ref : manifest.sstables()) {
      builder.append("sstable=").append(ref.level()).append(':').append(ref.sequenceNumber()).append('\n');
    }

    Path tmpPath = dataDirectory.resolve(MANIFEST_TMP_FILE);
    Path manifestPath = dataDirectory.resolve(MANIFEST_FILE);
    Files.writeString(tmpPath, builder.toString(), StandardCharsets.UTF_8);

    try {
      Files.move(tmpPath, manifestPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(tmpPath, manifestPath, StandardCopyOption.REPLACE_EXISTING);
    }

    logger.debug(
        "Wrote MANIFEST with {} SSTables, flushed_wal_sequence={}",
        manifest.sstables().size(),
        manifest.flushedWalSequence());
  }

  private static Manifest.SSTableRef parseSSTableRef(String value) throws IOException {
    int separator = value.indexOf(':');
    if (separator <= 0) {
      throw new IOException("Invalid sstable entry: " + value);
    }
    int level = Integer.parseInt(value.substring(0, separator));
    long sequenceNumber = Long.parseLong(value.substring(separator + 1));
    return new Manifest.SSTableRef(level, sequenceNumber);
  }
}
