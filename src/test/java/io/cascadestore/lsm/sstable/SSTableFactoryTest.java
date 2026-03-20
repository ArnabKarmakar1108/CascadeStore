package io.cascadestore.lsm.sstable;

import static org.junit.jupiter.api.Assertions.*;

import io.cascadestore.lsm.memtable.MemTable;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableFactoryTest {

  @TempDir Path tempDir;

  @Test
  void testCreateFromMemTable() throws IOException {
    // Arrange
    MemTable memTable = new MemTable(1024);
    String directory = tempDir.toString();
    int level = 1;
    long sequenceNumber = 123;

    // Act
    SSTableInterface sstable =
        SSTableFactory.createFromMemTable(memTable, directory, level, sequenceNumber);

    // Assert
    assertNotNull(sstable);
    assertTrue(sstable instanceof SSTableImpl);
    assertEquals(level, sstable.getLevel());
    assertEquals(sequenceNumber, sstable.getSequenceNumber());

    // Clean up
    sstable.close();
  }

  @Test
  void testOpenFromDisk() throws IOException {
    // Arrange
    // First create an SSTable to open
    MemTable memTable = new MemTable(1024);
    String directory = tempDir.toString();
    int level = 1;
    long sequenceNumber = 123;
    SSTableInterface createdSstable =
        SSTableFactory.createFromMemTable(memTable, directory, level, sequenceNumber);
    createdSstable.close();

    // Act
    SSTableInterface openedSstable = SSTableFactory.openFromDisk(directory, level, sequenceNumber);

    // Assert
    assertNotNull(openedSstable);
    assertTrue(openedSstable instanceof SSTableImpl);
    assertEquals(level, openedSstable.getLevel());
    assertEquals(sequenceNumber, openedSstable.getSequenceNumber());

    // Clean up
    openedSstable.close();
  }

  @Test
  void testCreateBackwardCompatibleWithMemTable() throws IOException {
    // Arrange
    MemTable memTable = new MemTable(1024);
    String directory = tempDir.toString();
    int level = 1;
    long sequenceNumber = 123;

    // Act
    SSTable sstable =
        SSTableFactory.createBackwardCompatible(memTable, directory, level, sequenceNumber);

    // Assert
    assertNotNull(sstable);
    assertTrue(sstable instanceof SSTableAdapter);
    assertEquals(level, sstable.getLevel());
    assertEquals(sequenceNumber, sstable.getSequenceNumber());

    // Clean up
    sstable.close();
  }

  @Test
  void testCreateBackwardCompatibleWithoutMemTable() throws IOException {
    // Arrange
    // First create an SSTable to open
    MemTable memTable = new MemTable(1024);
    String directory = tempDir.toString();
    int level = 1;
    long sequenceNumber = 123;
    SSTableInterface createdSstable =
        SSTableFactory.createFromMemTable(memTable, directory, level, sequenceNumber);
    createdSstable.close();

    // Act
    SSTable sstable =
        SSTableFactory.createBackwardCompatible(null, directory, level, sequenceNumber);

    // Assert
    assertNotNull(sstable);
    assertTrue(sstable instanceof SSTableAdapter);
    assertEquals(level, sstable.getLevel());
    assertEquals(sequenceNumber, sstable.getSequenceNumber());

    // Clean up
    sstable.close();
  }
}
