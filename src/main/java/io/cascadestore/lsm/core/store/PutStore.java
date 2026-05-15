package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.metrics.CascadeMetrics;
import io.cascadestore.lsm.wal.WAL;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PutStore {
  private static final Logger logger = LoggerFactory.getLogger(PutStore.class);

  public static final int RESULT_SUCCESS = 0;
  public static final int RESULT_INVALID_INPUT = 1;
  public static final int RESULT_WAL_ERROR = 2;
  public static final int RESULT_MEMTABLE_FULL = 3;

  // Dependencies
  private volatile MemTable activeMemTable;
  private ReadWriteLock memTableLock;
  private WAL wal;
  private AtomicBoolean recovering;
  private CascadeMetrics metrics;

  // For WAL errors
  private IOException lastException;

  public PutStore(
      MemTable activeMemTable, ReadWriteLock memTableLock, WAL wal, AtomicBoolean recovering) {
    this(activeMemTable, memTableLock, wal, recovering, CascadeMetrics.noop());
  }

  public PutStore(
      MemTable activeMemTable,
      ReadWriteLock memTableLock,
      WAL wal,
      AtomicBoolean recovering,
      CascadeMetrics metrics) {
    if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
    if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");
    if (wal == null) throw new IllegalArgumentException("wal cannot be null");
    if (recovering == null) throw new IllegalArgumentException("recovering cannot be null");

    this.activeMemTable = activeMemTable;
    this.memTableLock = memTableLock;
    this.wal = wal;
    this.recovering = recovering;
    this.metrics = metrics != null ? metrics : CascadeMetrics.noop();
  }

  public void updateDependencies(
      MemTable activeMemTable, ReadWriteLock memTableLock, WAL wal, AtomicBoolean recovering) {
    updateDependencies(activeMemTable, memTableLock, wal, recovering, metrics);
  }

  public void updateDependencies(
      MemTable activeMemTable,
      ReadWriteLock memTableLock,
      WAL wal,
      AtomicBoolean recovering,
      CascadeMetrics metrics) {
    if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
    if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");
    if (wal == null) throw new IllegalArgumentException("wal cannot be null");
    if (recovering == null) throw new IllegalArgumentException("recovering cannot be null");

    this.activeMemTable = activeMemTable;
    this.memTableLock = memTableLock;
    this.wal = wal;
    this.recovering = recovering;
    this.metrics = metrics != null ? metrics : CascadeMetrics.noop();
  }

  public IOException getLastException() {
    return lastException;
  }

  private void noteWalSequence(long walSequence) {
    memTableLock.readLock().lock();
    try {
      activeMemTable.noteWalSequence(walSequence);
    } finally {
      memTableLock.readLock().unlock();
    }
  }

  public int put(byte[] key, byte[] value, long ttlSeconds) {
    // Reset last exception
    lastException = null;

    // Validate input
    if (key == null || key.length == 0 || value == null) {
      return RESULT_INVALID_INPUT;
    }

    try {
      // Log the operation to WAL first (unless we're recovering)
      long walSequence;
      if (!recovering.get()) {
        walSequence = wal.appendPutRecord(key, value, ttlSeconds);
        noteWalSequence(walSequence);
      }

      // Try to put in the active MemTable
      int result = putInMemTable(key, value, ttlSeconds);
      if (result == RESULT_SUCCESS) {
        metrics.recordUserWriteBytes(key.length + value.length);
      }
      return result;
    } catch (IOException e) {
      logger.error("Error writing to WAL", e);
      lastException = e;
      return RESULT_WAL_ERROR;
    }
  }

  private int putInMemTable(byte[] key, byte[] value, long ttlSeconds) {
    boolean success = false;
    boolean needSwitch = false;

    // Acquire read lock for reading from MemTable
    memTableLock.readLock().lock();
    try {
      // Try to put in the active MemTable
      success = activeMemTable.put(key, value, ttlSeconds);

      // If the MemTable is full, we need to switch to a new one
      if (!success && activeMemTable.isFull()) {
        needSwitch = true;
      }
    } finally {
      memTableLock.readLock().unlock();
    }

    // If we need to switch MemTables, signal that to the caller
    if (needSwitch) {
      return RESULT_MEMTABLE_FULL;
    }
    return success ? RESULT_SUCCESS : RESULT_INVALID_INPUT;
  }
}
