package io.cascadestore.lsm.wal.record;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class PutRecordTest {

  @Test
  void testConstructorAndGetters() {
    // Arrange
    long sequenceNumber = 123;
    byte[] key = "testKey".getBytes();
    byte[] value = "testValue".getBytes();
    long ttlSeconds = 60;

    // Act
    PutRecord record = new PutRecord(sequenceNumber, key, value, ttlSeconds);

    // Assert
    assertEquals(sequenceNumber, record.getSequenceNumber());
    assertArrayEquals(key, record.getKey());
    assertArrayEquals(value, record.getValue());
    assertEquals(ttlSeconds, record.getTtlSeconds());
  }

  @Test
  void testDefensiveCopies() {
    // Arrange
    long sequenceNumber = 123;
    byte[] key = "testKey".getBytes();
    byte[] value = "testValue".getBytes();
    long ttlSeconds = 60;

    // Act
    PutRecord record = new PutRecord(sequenceNumber, key, value, ttlSeconds);

    // Modify the original arrays
    key[0] = 'X';
    value[0] = 'Y';

    // Assert that the record's copies are not affected
    assertNotEquals(key[0], record.getKey()[0]);
    assertNotEquals(value[0], record.getValue()[0]);

    // Modify the arrays returned by getters
    byte[] returnedKey = record.getKey();
    byte[] returnedValue = record.getValue();
    returnedKey[0] = 'Z';
    returnedValue[0] = 'W';

    // Assert that the record's internal state is not affected
    assertNotEquals(returnedKey[0], record.getKey()[0]);
    assertNotEquals(returnedValue[0], record.getValue()[0]);
  }

  @Test
  void testToString() {
    // Arrange
    long sequenceNumber = 123;
    byte[] key = "testKey".getBytes();
    byte[] value = "testValue".getBytes();
    long ttlSeconds = 60;

    // Act
    PutRecord record = new PutRecord(sequenceNumber, key, value, ttlSeconds);
    String toString = record.toString();

    // Assert
    assertTrue(toString.contains("sequenceNumber=" + sequenceNumber));
    assertTrue(toString.contains("key=" + Arrays.toString(key)));
    assertTrue(toString.contains("value=" + Arrays.toString(value)));
    assertTrue(toString.contains("ttlSeconds=" + ttlSeconds));
  }
}
