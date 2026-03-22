package io.cascadestore.lsm.sstable.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cascadestore.lsm.sstable.SSTableEntry;
import io.cascadestore.lsm.sstable.io.SSTableIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataFileManagerTest {

  @Mock private SSTableIO mockIO;

  @Mock private FileChannel mockChannel;

  private DataFileManagerImpl dataFileManager;

  @BeforeEach
  void setUp() {
    dataFileManager = new DataFileManagerImpl(mockIO);
  }

  @Test
  void testGetDataFilePath() {
    // Arrange
    when(mockIO.getDirectory()).thenReturn("/test/dir");
    when(mockIO.getLevel()).thenReturn(1);
    when(mockIO.getSequenceNumber()).thenReturn(123L);

    // Act
    Path result = dataFileManager.getDataFilePath();

    // Assert
    assertEquals("/test/dir/sst_L1_S123.data", result.toString());
    verify(mockIO).getDirectory();
    verify(mockIO).getLevel();
    verify(mockIO).getSequenceNumber();
  }

  @Test
  void testGetDataChannel() {
    // Arrange
    when(mockIO.getDataChannel()).thenReturn(mockChannel);

    // Act
    FileChannel result = dataFileManager.getDataChannel();

    // Assert
    assertSame(mockChannel, result);
    verify(mockIO).getDataChannel();
  }

  @Test
  void testWriteEntry() throws IOException {
    // Arrange
    SSTableEntry entry = SSTableEntry.of("key".getBytes(), "value".getBytes(), 123L);
    ByteBuffer buffer = ByteBuffer.allocate(10);
    when(mockIO.writeEntryHeader(entry)).thenReturn(buffer);
    when(mockIO.getDataChannel()).thenReturn(mockChannel);
    when(mockChannel.write(buffer)).thenReturn(5);

    // Act
    long result = dataFileManager.writeEntry(entry);

    // Assert
    assertEquals(5, result);
    verify(mockIO).writeEntryHeader(entry);
    verify(mockIO).getDataChannel();
    verify(mockChannel).write(buffer);
  }

  @Test
  void testClose() throws IOException {
    // Act & Assert
    assertDoesNotThrow(() -> dataFileManager.close());
  }
}
