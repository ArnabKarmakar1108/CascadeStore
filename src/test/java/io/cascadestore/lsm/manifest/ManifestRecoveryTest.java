package io.cascadestore.lsm.manifest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cascadestore.lsm.core.store.CascadeStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManifestRecoveryTest {

  @TempDir Path tempDir;

  @Test
  void restartUsesManifestCheckpointAndPreservesData() throws Exception {
    Path dataDir = tempDir.resolve("store");
    byte[] key = "checkpoint-key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "checkpoint-value".getBytes(StandardCharsets.UTF_8);

    CascadeStore store = new CascadeStore(1024, dataDir.toString(), 4);
    try {
      assertTrue(store.put(key, value));
    } finally {
      store.shutdown();
    }

    Path manifestPath = dataDir.resolve(ManifestStore.MANIFEST_FILE);
    assertTrue(Files.exists(manifestPath), "MANIFEST should exist after shutdown flush");

    ManifestStore manifestStore = new ManifestStore(dataDir.toString());
    Optional<Manifest> manifest = manifestStore.load();
    assertTrue(manifest.isPresent());
    assertTrue(manifest.get().flushedWalSequence() >= 0);
    assertTrue(!manifest.get().sstables().isEmpty());

    CascadeStore restarted = new CascadeStore(1024, dataDir.toString(), 4);
    try {
      assertArrayEquals(value, restarted.get(key));
    } finally {
      restarted.shutdown();
    }
  }

  @Test
  void secondRestartStillRecoversTailWrites() throws Exception {
    Path dataDir = tempDir.resolve("store-tail");
    byte[] key1 = "tail-1".getBytes(StandardCharsets.UTF_8);
    byte[] key2 = "tail-2".getBytes(StandardCharsets.UTF_8);

    CascadeStore first = new CascadeStore(1024, dataDir.toString(), 4);
    try {
      assertTrue(first.put(key1, "v1".getBytes(StandardCharsets.UTF_8)));
      first.shutdown();
    } finally {
      first.shutdown();
    }

    Manifest firstManifest = manifestStore(dataDir).orElseThrow();
    long firstCheckpoint = firstManifest.flushedWalSequence();

    CascadeStore second = new CascadeStore(1024, dataDir.toString(), 4);
    try {
      assertTrue(second.put(key2, "v2".getBytes(StandardCharsets.UTF_8)));
    } finally {
      second.shutdown();
    }

    Manifest secondManifest = manifestStore(dataDir).orElseThrow();
    assertTrue(secondManifest.flushedWalSequence() >= firstCheckpoint);

    CascadeStore third = new CascadeStore(1024, dataDir.toString(), 4);
    try {
      assertNotNull(third.get(key1));
      assertNotNull(third.get(key2));
    } finally {
      third.shutdown();
    }
  }

  private static Optional<Manifest> manifestStore(Path dataDir) throws Exception {
    return new ManifestStore(dataDir.toString()).load();
  }
}
