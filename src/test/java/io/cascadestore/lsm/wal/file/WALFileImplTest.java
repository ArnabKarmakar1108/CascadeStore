package io.cascadestore.lsm.wal.file;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WALFileImplTest {

  @TempDir Path tempDir;

  private Path walFilePath;
  private WALFileImpl walFile;
  private final long sequenceNumber = 123;

  @BeforeEach
  void setUp() throws IOException {
    walFilePath = tempDir.resolve("test_wal.log");
    walFile =
        new WALFileImpl(
            walFilePath,
            sequenceNumber,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (walFile != null) {
      walFile.close();
    }
  }

  @Test
  void testConstructorAndGetters() {
    assertEquals(walFilePath, walFile.getPath());
    assertEquals(sequenceNumber, walFile.getSequenceNumber());
    assertNotNull(walFile.getChannel());
    assertTrue(walFile.getChannel().isOpen());
  }

  @Test
  void testWriteAndRead() throws IOException {
    // Write data to the file
    ByteBuffer writeBuffer = ByteBuffer.wrap("test data".getBytes());
    int bytesWritten = walFile.write(writeBuffer);
    assertEquals(9, bytesWritten); // "test data" is 9 bytes

    // Read data from the file
    ByteBuffer readBuffer = ByteBuffer.allocate(9);
    int bytesRead = walFile.read(readBuffer, 0);
    assertEquals(9, bytesRead);

    // Verify the data
    readBuffer.flip();
    byte[] data = new byte[9];
    readBuffer.get(data);
    assertEquals("test data", new String(data));
  }

  @Test
  void testForce() throws IOException {
    // Write data to the file
    ByteBuffer writeBuffer = ByteBuffer.wrap("test data".getBytes());
    walFile.write(writeBuffer);

    // Force the data to disk
    assertDoesNotThrow(() -> walFile.force(true));
  }

  @Test
  void testSize() throws IOException {
    // Initially, the file should be empty
    assertEquals(0, walFile.size());

    // Write data to the file
    ByteBuffer writeBuffer = ByteBuffer.wrap("test data".getBytes());
    walFile.write(writeBuffer);

    // Check the size
    assertEquals(9, walFile.size()); // "test data" is 9 bytes
  }

  @Test
  void testClose() throws IOException {
    // Close the file
    walFile.close();

    // Verify the channel is closed
    assertFalse(walFile.getChannel().isOpen());

    // Closing again should not throw an exception
    assertDoesNotThrow(() -> walFile.close());
  }

  @Test
  void testToString() {
    String toString = walFile.toString();
    assertTrue(toString.contains("path=" + walFilePath));
    assertTrue(toString.contains("sequenceNumber=" + sequenceNumber));
  }
}
