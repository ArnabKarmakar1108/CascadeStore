package io.cascadestore.lsm.config;

import io.cascadestore.lsm.core.compaction.CompactionStrategyType;

public record CascadeConfig(
    int memTableMaxSizeBytes,
    String dataDirectory,
    int compactionThreshold,
    int compactionIntervalMinutes,
    int cleanupIntervalMinutes,
    int flushIntervalSeconds,
    CompactionStrategyType compactionStrategyType) {

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
  }

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
        this.compactionStrategyType);
  }

  public CascadeConfig withDataDirectory(String dataDirectory) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType);
  }

  public CascadeConfig withCompactionThreshold(int compactionThreshold) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType);
  }

  public CascadeConfig withCompactionIntervalMinutes(int compactionIntervalMinutes) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType);
  }

  public CascadeConfig withCleanupIntervalMinutes(int cleanupIntervalMinutes) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        this.compactionStrategyType);
  }

  public CascadeConfig withFlushIntervalSeconds(int flushIntervalSeconds) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        flushIntervalSeconds,
        this.compactionStrategyType);
  }

  public CascadeConfig withCompactionStrategyType(CompactionStrategyType compactionStrategyType) {
    return new CascadeConfig(
        this.memTableMaxSizeBytes,
        this.dataDirectory,
        this.compactionThreshold,
        this.compactionIntervalMinutes,
        this.cleanupIntervalMinutes,
        this.flushIntervalSeconds,
        compactionStrategyType);
  }
}
