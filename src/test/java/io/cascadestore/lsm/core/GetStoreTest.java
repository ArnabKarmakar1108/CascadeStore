package io.cascadestore.lsm.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.cascadestore.lsm.core.store.GetStore;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GetStoreTest {

  private GetStore getStore;
  private MemTable mockActiveMemTable;
  private List<MemTable> mockImmutableMemTables;
  private List<SSTable> mockSSTables;
  private ReadWriteLock readWriteLock;

  @BeforeEach
  void setUp() {
    mockActiveMemTable = Mockito.mock(MemTable.class);
    mockImmutableMemTables = new ArrayList<>();
    mockSSTables = new ArrayList<>();
    readWriteLock = new ReentrantReadWriteLock();

    // Create mock immutable MemTables
    for (int i = 0; i < 2; i++) {
      mockImmutableMemTables.add(Mockito.mock(MemTable.class));
    }

    // Create mock SSTables
    for (int i = 0; i < 2; i++) {
      mockSSTables.add(Mockito.mock(SSTable.class));
    }

    getStore =
        new GetStore(mockActiveMemTable, mockImmutableMemTables, mockSSTables, readWriteLock);
  }

  @Test
  void testGetWithNullKey() {
    int result = getStore.get(null);
    assertEquals(GetStore.RESULT_INVALID_INPUT, result);
    verifyNoInteractions(mockActiveMemTable);
    for (MemTable memTable : mockImmutableMemTables) {
      verifyNoInteractions(memTable);
    }
    for (SSTable ssTable : mockSSTables) {
      verifyNoInteractions(ssTable);
    }
  }

  @Test
  void testGetWithEmptyKey() {
    int result = getStore.get(new byte[0]);
    assertEquals(GetStore.RESULT_INVALID_INPUT, result);
    verifyNoInteractions(mockActiveMemTable);
    for (MemTable memTable : mockImmutableMemTables) {
      verifyNoInteractions(memTable);
    }
    for (SSTable ssTable : mockSSTables) {
      verifyNoInteractions(ssTable);
    }
  }

  @Test
  void testGetFromActiveMemTable() {
    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(expectedValue);

    int result = getStore.get(key);

    assertEquals(GetStore.RESULT_SUCCESS, result);
    assertArrayEquals(expectedValue, getStore.getRetrievedValue());

    verify(mockActiveMemTable).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verifyNoInteractions(memTable);
    }
    for (SSTable ssTable : mockSSTables) {
      verifyNoInteractions(ssTable);
    }
  }

  @Test
  void testGetFromImmutableMemTable() {
    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    when(mockImmutableMemTables.get(1).get(key)).thenReturn(expectedValue);

    int result = getStore.get(key);

    assertEquals(GetStore.RESULT_SUCCESS, result);
    assertArrayEquals(expectedValue, getStore.getRetrievedValue());

    verify(mockActiveMemTable).get(key);
    verify(mockImmutableMemTables.get(1)).get(key);
    for (SSTable ssTable : mockSSTables) {
      verifyNoInteractions(ssTable);
    }
  }

  @Test
  void testGetFromSSTable() {
    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    for (MemTable memTable : mockImmutableMemTables) {
      when(memTable.get(key)).thenReturn(null);
    }

    when(mockSSTables.get(1).mightContain(key)).thenReturn(true);
    when(mockSSTables.get(1).get(key)).thenReturn(expectedValue);

    int result = getStore.get(key);

    assertEquals(GetStore.RESULT_SUCCESS, result);
    assertArrayEquals(expectedValue, getStore.getRetrievedValue());

    verify(mockActiveMemTable).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verify(memTable).get(key);
    }
    verify(mockSSTables.get(1)).mightContain(key);
    verify(mockSSTables.get(1)).get(key);
  }

  @Test
  void testGetKeyNotFound() {
    byte[] key = "key".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    for (MemTable memTable : mockImmutableMemTables) {
      when(memTable.get(key)).thenReturn(null);
    }
    for (SSTable ssTable : mockSSTables) {
      when(ssTable.mightContain(key)).thenReturn(false);
    }

    int result = getStore.get(key);

    assertEquals(GetStore.RESULT_KEY_NOT_FOUND, result);
    assertNull(getStore.getRetrievedValue());

    verify(mockActiveMemTable).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verify(memTable).get(key);
    }
    for (SSTable ssTable : mockSSTables) {
      verify(ssTable).mightContain(key);
      verify(ssTable, never()).get(key);
    }
  }

  @Test
  void testGetWithBloomFilterOptimization() {
    byte[] key = "key".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    for (MemTable memTable : mockImmutableMemTables) {
      when(memTable.get(key)).thenReturn(null);
    }

    // First SSTable says it might contain the key but doesn't
    when(mockSSTables.get(1).mightContain(key)).thenReturn(true);
    when(mockSSTables.get(1).get(key)).thenReturn(null);

    // Second SSTable says it definitely doesn't contain the key
    when(mockSSTables.get(0).mightContain(key)).thenReturn(false);

    int result = getStore.get(key);

    assertEquals(GetStore.RESULT_KEY_NOT_FOUND, result);
    assertNull(getStore.getRetrievedValue());

    verify(mockActiveMemTable).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verify(memTable).get(key);
    }
    verify(mockSSTables.get(1)).mightContain(key);
    verify(mockSSTables.get(1)).get(key);
    verify(mockSSTables.get(0)).mightContain(key);
    verify(mockSSTables.get(0), never())
        .get(key); // Should never call get() if mightContain() returns false
  }

  @Test
  void testUpdateDependencies() {
    // Create new mocks
    MemTable newMockMemTable = Mockito.mock(MemTable.class);
    List<MemTable> newMockImmutableMemTables = new ArrayList<>();
    newMockImmutableMemTables.add(Mockito.mock(MemTable.class));
    List<SSTable> newMockSSTables = new ArrayList<>();
    newMockSSTables.add(Mockito.mock(SSTable.class));
    ReadWriteLock newReadWriteLock = new ReentrantReadWriteLock();

    // Update dependencies
    getStore.updateDependencies(
        newMockMemTable, newMockImmutableMemTables, newMockSSTables, newReadWriteLock);

    // Test that the new dependencies are used
    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(newMockMemTable.get(key)).thenReturn(expectedValue);

    int result = getStore.get(key);

    assertEquals(GetStore.RESULT_SUCCESS, result);
    assertArrayEquals(expectedValue, getStore.getRetrievedValue());

    verify(newMockMemTable).get(key);
    verifyNoInteractions(mockActiveMemTable); // Old mock should not be used
  }
}
