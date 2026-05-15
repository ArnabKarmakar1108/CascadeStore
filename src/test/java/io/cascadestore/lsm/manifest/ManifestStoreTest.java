package io.cascadestore.lsm.manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManifestStoreTest {

  @TempDir Path tempDir;

  @Test
  void roundTripManifest() throws Exception {
    ManifestStore store = new ManifestStore(tempDir.toString());
    Manifest manifest =
        new Manifest(
            42,
            List.of(new Manifest.SSTableRef(0, 1), new Manifest.SSTableRef(1, 7)));

    store.save(manifest);

    Optional<Manifest> loaded = store.load();
    assertTrue(loaded.isPresent());
    assertEquals(manifest, loaded.get());
  }
}
