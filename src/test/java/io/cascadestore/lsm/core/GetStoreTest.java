package io.cascadestore.lsm.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.cascadestore.lsm.core.store.GetStore;
import io.cascadestore.lsm.core.store.StorageVersion;
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
  private StorageVersion storageVersion;

  @BeforeEach
  void setUp() {
    mockActiveMemTable = Mockito.mock(MemTable.class);
    mockImmutableMemTables = new ArrayList<>();
    mockSSTables = new ArrayList<>();
    readWriteLock = new ReentrantReadWriteLock();

    for (int i = 0; i < 2; i++) {
      mockImmutableMemTables.add(Mockito.mock(MemTable.class));
    }

    for (int i = 0; i < 2; i++) {
      mockSSTables.add(Mockito.mock(SSTable.class));
    }

    storageVersion = new StorageVersion(1L, mockImmutableMemTables, mockSSTables);
    getStore = new GetStore(mockActiveMemTable, storageVersion, readWriteLock);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() {
    if (storageVersion != null) {
      storageVersion.release();
    }
  }

  @Test
  void testGetWithNullKey() {
    assertEquals(GetStore.RESULT_INVALID_INPUT, getStore.get(null));
    assertNull(getStore.lookup(null));
    verifyNoInteractions(mockActiveMemTable);
  }

  @Test
  void testGetWithEmptyKey() {
    assertEquals(GetStore.RESULT_INVALID_INPUT, getStore.get(new byte[0]));
    assertNull(getStore.lookup(new byte[0]));
    verifyNoInteractions(mockActiveMemTable);
  }

  @Test
  void testGetFromActiveMemTable() {
    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(expectedValue);

    assertArrayEquals(expectedValue, getStore.lookup(key));
    assertEquals(GetStore.RESULT_SUCCESS, getStore.get(key));

    verify(mockActiveMemTable, atLeastOnce()).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verifyNoInteractions(memTable);
    }
    for (SSTable ssTable : mockSSTables) {
      verify(ssTable, never()).get(key);
    }
  }

  @Test
  void testGetFromImmutableMemTable() {
    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    when(mockActiveMemTable.shadows(key)).thenReturn(false);
    when(mockImmutableMemTables.get(1).shadows(key)).thenReturn(true);
    when(mockImmutableMemTables.get(1).get(key)).thenReturn(expectedValue);

    assertArrayEquals(expectedValue, getStore.lookup(key));

    verify(mockActiveMemTable).get(key);
    verify(mockActiveMemTable).shadows(key);
    verify(mockImmutableMemTables.get(1)).shadows(key);
    verify(mockImmutableMemTables.get(1)).get(key);
    for (SSTable ssTable : mockSSTables) {
      verify(ssTable, never()).get(key);
    }
  }

  @Test
  void testGetFromSSTable() {
    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    when(mockActiveMemTable.shadows(key)).thenReturn(false);
    for (MemTable memTable : mockImmutableMemTables) {
      when(memTable.shadows(key)).thenReturn(false);
    }

    when(mockSSTables.get(1).mightContain(key)).thenReturn(true);
    when(mockSSTables.get(1).get(key)).thenReturn(expectedValue);

    assertArrayEquals(expectedValue, getStore.lookup(key));

    verify(mockActiveMemTable).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verify(memTable).shadows(key);
    }
    verify(mockSSTables.get(1)).mightContain(key);
    verify(mockSSTables.get(1)).get(key);
    verify(mockSSTables.get(1), never()).unpin();
  }

  @Test
  void testGetKeyNotFound() {
    byte[] key = "key".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    when(mockActiveMemTable.shadows(key)).thenReturn(false);
    for (MemTable memTable : mockImmutableMemTables) {
      when(memTable.shadows(key)).thenReturn(false);
    }
    for (SSTable ssTable : mockSSTables) {
      when(ssTable.mightContain(key)).thenReturn(false);
    }

    assertNull(getStore.lookup(key));
    assertEquals(GetStore.RESULT_KEY_NOT_FOUND, getStore.get(key));

    verify(mockActiveMemTable, atLeastOnce()).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verify(memTable, atLeastOnce()).shadows(key);
    }
    for (SSTable ssTable : mockSSTables) {
      verify(ssTable, atLeastOnce()).mightContain(key);
      verify(ssTable, never()).get(key);
    }
  }

  @Test
  void testGetWithBloomFilterOptimization() {
    byte[] key = "key".getBytes();

    when(mockActiveMemTable.get(key)).thenReturn(null);
    when(mockActiveMemTable.shadows(key)).thenReturn(false);
    for (MemTable memTable : mockImmutableMemTables) {
      when(memTable.shadows(key)).thenReturn(false);
    }

    when(mockSSTables.get(1).mightContain(key)).thenReturn(true);
    when(mockSSTables.get(1).get(key)).thenReturn(null);
    when(mockSSTables.get(0).mightContain(key)).thenReturn(false);

    assertNull(getStore.lookup(key));

    verify(mockActiveMemTable).get(key);
    for (MemTable memTable : mockImmutableMemTables) {
      verify(memTable).shadows(key);
    }
    verify(mockSSTables.get(1)).mightContain(key);
    verify(mockSSTables.get(1)).get(key);
    verify(mockSSTables.get(0)).mightContain(key);
    verify(mockSSTables.get(0), never()).get(key);
  }

  @Test
  void testUpdateDependencies() {
    MemTable newMockMemTable = Mockito.mock(MemTable.class);
    List<MemTable> newMockImmutableMemTables = new ArrayList<>();
    newMockImmutableMemTables.add(Mockito.mock(MemTable.class));
    List<SSTable> newMockSSTables = new ArrayList<>();
    newMockSSTables.add(Mockito.mock(SSTable.class));
    ReadWriteLock newReadWriteLock = new ReentrantReadWriteLock();
    StorageVersion newVersion =
        new StorageVersion(2L, newMockImmutableMemTables, newMockSSTables);

    getStore.updateDependencies(newMockMemTable, newVersion, newReadWriteLock);
    storageVersion.release();
    storageVersion = newVersion;

    byte[] key = "key".getBytes();
    byte[] expectedValue = "value".getBytes();

    when(newMockMemTable.get(key)).thenReturn(expectedValue);

    assertArrayEquals(expectedValue, getStore.lookup(key));

    verify(newMockMemTable).get(key);
    verifyNoInteractions(mockActiveMemTable);
  }
}
