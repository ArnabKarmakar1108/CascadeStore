package io.cascadestore.lsm.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import io.cascadestore.lsm.core.store.DeleteStore;
import io.cascadestore.lsm.core.store.GetStore;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.wal.WAL;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DeleteStoreTest {

  private DeleteStore deleteStore;
  private MemTable mockMemTable;
  private ReadWriteLock readWriteLock;
  private WAL mockWal;
  private AtomicBoolean recovering;
  private GetStore mockGetStore;

  @BeforeEach
  void setUp() {
    mockMemTable = Mockito.mock(MemTable.class);
    readWriteLock = new ReentrantReadWriteLock();
    mockWal = Mockito.mock(WAL.class);
    recovering = new AtomicBoolean(false);
    mockGetStore = Mockito.mock(GetStore.class);

    deleteStore = new DeleteStore(mockMemTable, readWriteLock, mockWal, recovering, mockGetStore);
  }

  @Test
  void testDeleteWithNullKey() {
    int result = deleteStore.delete(null);
    assertEquals(DeleteStore.RESULT_INVALID_INPUT, result);
    verifyNoInteractions(mockWal, mockMemTable, mockGetStore);
  }

  @Test
  void testDeleteWithEmptyKey() {
    int result = deleteStore.delete(new byte[0]);
    assertEquals(DeleteStore.RESULT_INVALID_INPUT, result);
    verifyNoInteractions(mockWal, mockMemTable, mockGetStore);
  }

  @Test
  void testDeleteKeyNotFound() {
    byte[] key = "key".getBytes();

    // Mock GetStore to return KeyNotFound
    when(mockGetStore.exists(key)).thenReturn(GetStore.RESULT_KEY_NOT_FOUND);

    int result = deleteStore.delete(key);

    assertEquals(DeleteStore.RESULT_KEY_NOT_FOUND, result);
    verify(mockGetStore).exists(key);
    verifyNoInteractions(mockWal, mockMemTable);
  }

  @Test
  void testSuccessfulDelete() throws IOException {
    byte[] key = "key".getBytes();

    // Mock GetStore to return Success
    when(mockGetStore.exists(key)).thenReturn(GetStore.RESULT_SUCCESS);
    when(mockMemTable.delete(key)).thenReturn(true);

    int result = deleteStore.delete(key);

    assertEquals(DeleteStore.RESULT_SUCCESS, result);
    verify(mockGetStore).exists(key);
    verify(mockWal).appendDeleteRecord(key);
    verify(mockMemTable).delete(key);
  }

  @Test
  void testDeleteWithWALError() throws IOException {
    byte[] key = "key".getBytes();

    // Mock GetStore to return Success
    when(mockGetStore.exists(key)).thenReturn(GetStore.RESULT_SUCCESS);
    doThrow(new IOException("WAL error")).when(mockWal).appendDeleteRecord(key);

    int result = deleteStore.delete(key);

    assertEquals(DeleteStore.RESULT_WAL_ERROR, result);
    verify(mockGetStore).exists(key);
    verify(mockWal).appendDeleteRecord(key);
    verifyNoInteractions(mockMemTable);
    assertNotNull(deleteStore.getLastException());
  }

  @Test
  void testDeleteWithFullMemTable() throws IOException {
    byte[] key = "key".getBytes();

    // Mock GetStore to return Success
    when(mockGetStore.exists(key)).thenReturn(GetStore.RESULT_SUCCESS);
    when(mockMemTable.delete(key)).thenReturn(false);
    when(mockMemTable.isFull()).thenReturn(true);

    int result = deleteStore.delete(key);

    assertEquals(DeleteStore.RESULT_MEMTABLE_FULL, result);
    verify(mockGetStore).exists(key);
    verify(mockWal).appendDeleteRecord(key);
    verify(mockMemTable).delete(key);
    verify(mockMemTable).isFull();
  }

  @Test
  void testDeleteDuringRecovery() throws IOException {
    byte[] key = "key".getBytes();

    // Set recovering to true
    recovering.set(true);

    // Mock GetStore to return Success
    when(mockGetStore.exists(key)).thenReturn(GetStore.RESULT_SUCCESS);
    when(mockMemTable.delete(key)).thenReturn(true);

    int result = deleteStore.delete(key);

    assertEquals(DeleteStore.RESULT_SUCCESS, result);
    verify(mockGetStore).exists(key);
    verifyNoInteractions(mockWal); // WAL should not be called during recovery
    verify(mockMemTable).delete(key);
  }

  @Test
  void testUpdateDependencies() {
    // Create new mocks
    MemTable newMockMemTable = Mockito.mock(MemTable.class);
    ReadWriteLock newReadWriteLock = new ReentrantReadWriteLock();
    WAL newMockWal = Mockito.mock(WAL.class);
    AtomicBoolean newRecovering = new AtomicBoolean(true);
    GetStore newMockGetStore = Mockito.mock(GetStore.class);

    // Update dependencies
    deleteStore.updateDependencies(
        newMockMemTable, newReadWriteLock, newMockWal, newRecovering, newMockGetStore);

    // Test that the new dependencies are used
    byte[] key = "key".getBytes();

    when(newMockGetStore.exists(key)).thenReturn(GetStore.RESULT_SUCCESS);
    when(newMockMemTable.delete(key)).thenReturn(true);

    int result = deleteStore.delete(key);

    assertEquals(DeleteStore.RESULT_SUCCESS, result);
    verify(newMockGetStore).exists(key);
    verifyNoInteractions(
        newMockWal); // WAL should not be called during recovery (newRecovering is true)
    verify(newMockMemTable).delete(key);
    verifyNoInteractions(mockGetStore); // Old mock should not be used
  }
}
