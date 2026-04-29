package io.cascadestore.lsm.io;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BufferedDataReaderTest {

  @TempDir Path tempDir;

  @Test
  void readsSequentialFieldsFromSingleBufferWindow() throws Exception {
    Path file = tempDir.resolve("data.bin");
    try (FileChannel channel =
        FileChannel.open(
            file,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ)) {
      ByteBuffer payload = ByteBuffer.allocate(32);
      payload.putInt(3);
      payload.put("key".getBytes());
      payload.putInt(5);
      payload.put("value".getBytes());
      payload.flip();
      channel.write(payload);
    }

    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        BufferedDataReader reader = new BufferedDataReader(channel, 16)) {
      reader.seek(0);
      assertEquals(3, reader.readInt());
      assertArrayEquals("key".getBytes(), reader.readBytes(3));
      assertEquals(5, reader.readInt());
      assertArrayEquals("value".getBytes(), reader.readBytes(5));
      assertEquals(16, reader.position());
    }
  }

  @Test
  void refillsAcrossBufferBoundaries() throws Exception {
    Path file = tempDir.resolve("large.bin");
    byte[] blob = new byte[128];
    for (int i = 0; i < blob.length; i++) {
      blob[i] = (byte) i;
    }
    Files.write(file, blob);

    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        BufferedDataReader reader = new BufferedDataReader(channel, 32, null, 0L, null)) {
      reader.seek(30);
      byte[] first = reader.readBytes(8);
      byte[] second = reader.readBytes(8);
      assertArrayEquals(java.util.Arrays.copyOfRange(blob, 30, 38), first);
      assertArrayEquals(java.util.Arrays.copyOfRange(blob, 38, 46), second);
    }
  }

  @Test
  void usesMappedBackingWhenAvailable() throws Exception {
    Path file = tempDir.resolve("mapped.bin");
    byte[] payload = "mapped-read".getBytes();
    Files.write(file, payload);

    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        BufferedDataReader reader = new BufferedDataReader(channel, 8)) {
      assertTrue(reader.isMapped());
      reader.seek(0);
      assertArrayEquals(payload, reader.readBytes(payload.length));
    }
  }

  @Test
  void prefetchLoadsNextWindowWithoutChangingPosition() throws Exception {
    byte[] blob = new byte[96];
    for (int i = 0; i < blob.length; i++) {
      blob[i] = (byte) i;
    }
    Path file = tempDir.resolve("prefetch.bin");
    Files.write(file, blob);

    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        BufferedDataReader reader = new BufferedDataReader(channel, 32, null, 0L, null)) {
      reader.seek(0);
      assertEquals(0, reader.position());
      reader.prefetch(0);
      reader.seek(32);
      byte[] secondWindow = reader.readBytes(8);
      assertArrayEquals(java.util.Arrays.copyOfRange(blob, 32, 40), secondWindow);
    }
  }
}
