package io.cascadestore.lsm.sstable;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SSTableEntryTest {

  @Test
  void testCreateRegularEntry() {
    // Create a regular entry using the of() method
    byte[] key = "testKey".getBytes();
    byte[] value = "testValue".getBytes();
    long timestamp = System.currentTimeMillis();

    SSTableEntry entry = SSTableEntry.of(key, value, timestamp);

    // Verify the entry properties
    assertArrayEquals(key, entry.key());
    assertArrayEquals(value, entry.value());
    assertEquals(timestamp, entry.timestamp());
    assertFalse(entry.tombstone());
  }

  @Test
  void testCreateTombstoneEntry() {
    // Create a tombstone entry using the tombstone() method
    byte[] key = "deletedKey".getBytes();
    long timestamp = System.currentTimeMillis();

    SSTableEntry entry = SSTableEntry.tombstone(key, timestamp);

    // Verify the entry properties
    assertArrayEquals(key, entry.key());
    assertNull(entry.value());
    assertEquals(timestamp, entry.timestamp());
    assertTrue(entry.tombstone());
  }

  @Test
  void testIsNewerThan() {
    // Create two entries with different timestamps
    byte[] key = "key".getBytes();
    byte[] value = "value".getBytes();
    long olderTimestamp = System.currentTimeMillis();
    long newerTimestamp = olderTimestamp + 1000; // 1 second later

    SSTableEntry olderEntry = SSTableEntry.of(key, value, olderTimestamp);
    SSTableEntry newerEntry = SSTableEntry.of(key, value, newerTimestamp);

    // Test isNewerThan method
    assertTrue(newerEntry.isNewerThan(olderEntry));
    assertFalse(olderEntry.isNewerThan(newerEntry));

    // An entry is not newer than itself
    assertFalse(olderEntry.isNewerThan(olderEntry));
  }

  @Test
  void testRecordComponents() {
    // Create an entry
    byte[] key = "key".getBytes();
    byte[] value = "value".getBytes();
    long timestamp = System.currentTimeMillis();
    boolean tombstone = false;

    SSTableEntry entry = new SSTableEntry(key, value, timestamp, tombstone);

    // Test accessing record components
    assertArrayEquals(key, entry.key());
    assertArrayEquals(value, entry.value());
    assertEquals(timestamp, entry.timestamp());
    assertEquals(tombstone, entry.tombstone());

    // Verify that the record components are immutable
    // Modifying the original arrays should not affect the record
    key[0] = 'K'; // Change first byte
    value[0] = 'V'; // Change first byte

    // The record should still have the original values
    assertFalse(Arrays.equals(key, entry.key()));
    assertFalse(Arrays.equals(value, entry.value()));
  }

  @Test
  void testEquality() {
    // Create two identical entries
    byte[] key1 = "key".getBytes();
    byte[] value1 = "value".getBytes();
    long timestamp = System.currentTimeMillis();

    SSTableEntry entry1 = SSTableEntry.of(key1, value1, timestamp);
    SSTableEntry entry2 = SSTableEntry.of(key1.clone(), value1.clone(), timestamp);

    // Test equality
    assertEquals(entry1, entry2);
    assertEquals(entry1.hashCode(), entry2.hashCode());

    // Create an entry with different key
    byte[] key2 = "differentKey".getBytes();
    SSTableEntry entry3 = SSTableEntry.of(key2, value1, timestamp);

    // Test inequality
    assertNotEquals(entry1, entry3);

    // Create an entry with different value
    byte[] value2 = "differentValue".getBytes();
    SSTableEntry entry4 = SSTableEntry.of(key1, value2, timestamp);

    // Test inequality
    assertNotEquals(entry1, entry4);

    // Create an entry with different timestamp
    SSTableEntry entry5 = SSTableEntry.of(key1, value1, timestamp + 1000);

    // Test inequality
    assertNotEquals(entry1, entry5);

    // Create a tombstone entry
    SSTableEntry entry6 = SSTableEntry.tombstone(key1, timestamp);

    // Test inequality
    assertNotEquals(entry1, entry6);
  }
}
