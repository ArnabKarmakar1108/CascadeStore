package io.cascadestore.lsm.sstable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SSTableAdapterTest {

  @Mock private SSTableInterface mockDelegate;

  private SSTableAdapter adapter;

  @BeforeEach
  void setUp() throws IOException {
    adapter = new SSTableAdapter(mockDelegate);
  }

  @Test
  void testGet() {
    // Arrange
    byte[] key = "testKey".getBytes();
    byte[] value = "testValue".getBytes();
    when(mockDelegate.get(key)).thenReturn(value);

    // Act
    byte[] result = adapter.get(key);

    // Assert
    assertArrayEquals(value, result);
    verify(mockDelegate).get(key);
  }

  @Test
  void testMightContain() {
    // Arrange
    byte[] key = "testKey".getBytes();
    when(mockDelegate.mightContain(key)).thenReturn(true);

    // Act
    boolean result = adapter.mightContain(key);

    // Assert
    assertTrue(result);
    verify(mockDelegate).mightContain(key);
  }

  @Test
  void testGetLevel() {
    // Arrange
    int level = 1;
    when(mockDelegate.getLevel()).thenReturn(level);

    // Act
    int result = adapter.getLevel();

    // Assert
    assertEquals(level, result);
    verify(mockDelegate).getLevel();
  }

  @Test
  void testGetSequenceNumber() {
    // Arrange
    long sequenceNumber = 123;
    when(mockDelegate.getSequenceNumber()).thenReturn(sequenceNumber);

    // Act
    long result = adapter.getSequenceNumber();

    // Assert
    assertEquals(sequenceNumber, result);
    verify(mockDelegate).getSequenceNumber();
  }

  @Test
  void testGetCreationTime() {
    // Arrange
    long creationTime = System.currentTimeMillis();
    when(mockDelegate.getCreationTime()).thenReturn(creationTime);

    // Act
    long result = adapter.getCreationTime();

    // Assert
    assertEquals(creationTime, result);
    verify(mockDelegate).getCreationTime();
  }

  @Test
  void testGetSizeBytes() {
    // Arrange
    long sizeBytes = 1024;
    when(mockDelegate.getSizeBytes()).thenReturn(sizeBytes);

    // Act
    long result = adapter.getSizeBytes();

    // Assert
    assertEquals(sizeBytes, result);
    verify(mockDelegate).getSizeBytes();
  }

  @Test
  void testClose() throws IOException {
    // Act
    adapter.close();

    // Assert
    verify(mockDelegate).close();
  }

  @Test
  void testDelete() {
    // Arrange
    when(mockDelegate.delete()).thenReturn(true);

    // Act
    boolean result = adapter.delete();

    // Assert
    assertTrue(result);
    verify(mockDelegate).delete();
  }

  @Test
  void testListKeys() {
    // Arrange
    List<byte[]> keys = List.of("key1".getBytes(), "key2".getBytes());
    when(mockDelegate.listKeys()).thenReturn(keys);

    // Act
    List<byte[]> result = adapter.listKeys();

    // Assert
    assertEquals(keys, result);
    verify(mockDelegate).listKeys();
  }

  @Test
  void testCountEntries() {
    // Arrange
    int count = 10;
    when(mockDelegate.countEntries()).thenReturn(count);

    // Act
    int result = adapter.countEntries();

    // Assert
    assertEquals(count, result);
    verify(mockDelegate).countEntries();
  }

  @Test
  void testGetRange() {
    // Arrange
    byte[] startKey = "start".getBytes();
    byte[] endKey = "end".getBytes();
    Map<byte[], byte[]> rangeMap =
        Map.of(
            "key1".getBytes(), "value1".getBytes(),
            "key2".getBytes(), "value2".getBytes());
    when(mockDelegate.getRange(startKey, endKey)).thenReturn(rangeMap);

    // Act
    Map<byte[], byte[]> result = adapter.getRange(startKey, endKey);

    // Assert
    assertEquals(rangeMap, result);
    verify(mockDelegate).getRange(startKey, endKey);
  }

  @Test
  void testGetDelegate() {
    // Act
    SSTableInterface result = adapter.getDelegate();

    // Assert
    assertSame(mockDelegate, result);
  }
}
