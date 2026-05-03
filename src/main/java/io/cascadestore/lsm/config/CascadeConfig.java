package io.cascadestore.lsm.config;

import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.io.BlockCache;
import java.util.concurrent.TimeUnit;

public record CascadeConfig(
    int memTableMaxSizeBytes,
    String dataDirectory,
    int compactionThreshold,
    double compactionIntervalMinutes,
    int cleanupIntervalMinutes,
    int flushIntervalSeconds,
    CompactionStrategyType compactionStrategyType,
    int blockCacheSizeBytes,
    boolean parallelBloomEnabled,
    int parallelBloomMinTables) {

  public CascadeConfig(
      int memTableMaxSizeBytes,
      String dataDirectory,
      int compactionThreshold,
      double compactionIntervalMinutes,
      int cleanupIntervalMinutes,
      int flushIntervalSeconds,
      CompactionStrategyType compactionStrategyType) {
    this(
        memTableMaxSizeBytes,
        dataDirectory,
        compactionThreshold,
        compactionIntervalMinutes,
        cleanupIntervalMinutes,
        flushIntervalSeconds,
        compactionStrategyType,
        BlockCache.DEFAULT_SIZE_BYTES,
        true,
        3);
  }

  public CascadeConfig(
      int memTableMaxSizeBytes,
      String dataDirectory,
      int compactionThreshold,
      double compactionIntervalMinutes,
      int cleanupIntervalMinutes,
      int flushIntervalSeconds,
      CompactionStrategyType compactionStrategyType,
      int blockCacheSizeBytes) {
    this(
        memTableMaxSizeBytes,
        dataDirectory,
        compactionThreshold,
        compactionIntervalMinutes,
        cleanupIntervalMinutes,
        flushIntervalSeconds,
        compactionStrategyType,
        blockCacheSizeBytes,
        true,
        3);
  }

  public CascadeConfig {
    if (memTableMaxSizeBytes <= 0) {
      throw new IllegalArgumentException("memTableMaxSizeBytes must be positive");
    }
    if (dataDirectory == null || dataDirectory.isEmpty()) {
      throw new IllegalArgumentException("dataDirectory must not be null or empty");
    }
    if (compactionThreshold <= 0) {
      throw new IllegalArgumentException("compactionThreshold must be positive");
    }
    if (compactionIntervalMinutes <= 0) {
      throw new IllegalArgumentException("compactionIntervalMinutes must be positive");
    }
    if (cleanupIntervalMinutes <= 0) {
      throw new IllegalArgumentException("cleanupIntervalMinutes must be positive");
    }
    if (flushIntervalSeconds <= 0) {
      throw new IllegalArgumentException("flushIntervalSeconds must be positive");
    }
    if (compactionStrategyType == null) {
      throw new IllegalArgumentException("compactionStrategyType must not be null");
    }
    if (blockCacheSizeBytes < 0) {
      throw new IllegalArgumentException("blockCacheSizeBytes must be non-negative");
    }
    if (parallelBloomMinTables <= 0) {
      throw new IllegalArgumentException("parallelBloomMinTables must be positive");
    }
  }

  /**
   * Compaction schedule derived from {@link #compactionIntervalMinutes()}.
   *
   * <p>Values {@code >= 1} are minutes ({@code 30} = 30 min, {@code 10} = 10 min). Values
   * {@code < 1} are seconds ({@code 0.5} = 1 s after clamping).
   */
  public CompactionInterval compactionInterval() {
    long periodSeconds;
    if (compactionIntervalMinutes >= 1.0) {
      periodSeconds = Math.max(1L, Math.round(compactionIntervalMinutes * 60.0));
    } else {
      periodSeconds = Math.max(1L, Math.round(compactionIntervalMinutes));
    }

    long initialDelay = compactionIntervalMinutes >= 1.0
        ? Math.min(30L, Math.max(1L, periodSeconds / 3L))
        : Math.max(1L, periodSeconds / 3L);
    return new CompactionInterval(initialDelay, periodSeconds, TimeUnit.SECONDS);
  }

  public record CompactionInterval(long initialDelay, long period, TimeUnit unit) {}

  public static CascadeConfig getDefault() {
    return new CascadeConfig(
        10 * 1024 * 1024, "./data", 4, 30, 1, 10, CompactionStrategyType.THRESHOLD);
  }

  public CascadeConfig withMemTableMaxSizeBytes(int memTableMaxSizeBytes) {
    return new CascadeConfig(
        memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withDataDirectory(String dataDirectory) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withCompactionThreshold(int compactionThreshold) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withCompactionIntervalMinutes(double compactionIntervalMinutes) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withCleanupIntervalMinutes(int cleanupIntervalMinutes) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withFlushIntervalSeconds(int flushIntervalSeconds) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withCompactionStrategyType(CompactionStrategyType compactionStrategyType) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withBlockCacheSizeBytes(int blockCacheSizeBytes) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        blockCacheSizeBytes,
        this.parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withParallelBloomEnabled(boolean parallelBloomEnabled) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        parallelBloomEnabled,
        this.parallelBloomMinTables);
  }

  public CascadeConfig withParallelBloomMinTables(int parallelBloomMinTables) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType,
        this.blockCacheSizeBytes,
        this.parallelBloomEnabled,
        parallelBloomMinTables);
  }
}
