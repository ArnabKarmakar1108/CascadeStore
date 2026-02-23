package io.cascadestore.lsm.wal.record;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class DeleteRecordTest {

  @Test
  void testConstructorAndGetters() {
    // Arrange
    long sequenceNumber = 123;
    byte[] key = "testKey".getBytes();

    // Act
    DeleteRecord record = new DeleteRecord(sequenceNumber, key);

    // Assert
    assertEquals(sequenceNumber, record.getSequenceNumber());
    assertArrayEquals(key, record.getKey());
  }

  @Test
  void testDefensiveCopies() {
    // Arrange
    long sequenceNumber = 123;
    byte[] key = "testKey".getBytes();

    // Act
    DeleteRecord record = new DeleteRecord(sequenceNumber, key);

    // Modify the original array
    key[0] = 'X';

    // Assert that the record's copy is not affected
    assertNotEquals(key[0], record.getKey()[0]);

    // Modify the array returned by getter
    byte[] returnedKey = record.getKey();
    returnedKey[0] = 'Z';

    // Assert that the record's internal state is not affected
    assertNotEquals(returnedKey[0], record.getKey()[0]);
  }

  @Test
  void testToString() {
    // Arrange
    long sequenceNumber = 123;
    byte[] key = "testKey".getBytes();

    // Act
    DeleteRecord record = new DeleteRecord(sequenceNumber, key);
    String toString = record.toString();

    // Assert
    assertTrue(toString.contains("sequenceNumber=" + sequenceNumber));
    assertTrue(toString.contains("key=" + Arrays.toString(key)));
  }
}
