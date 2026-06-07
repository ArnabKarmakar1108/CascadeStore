package io.cascadestore.lsm.io.compression;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class Lz4CompressionTest {

  @Test
  void roundTripLargePayload() {
    byte[] input = new byte[2048];
    Arrays.fill(input, (byte) 'x');
    byte[] compressed = Lz4Compression.compressIfBeneficial(input);
    assertNotNull(compressed);
    assertArrayEquals(input, Lz4Compression.decompress(compressed, input.length));
  }

  @Test
  void skipsTinyPayloads() {
    byte[] input = new byte[32];
    assertNull(Lz4Compression.compressIfBeneficial(input));
  }
}
