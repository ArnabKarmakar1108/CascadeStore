package io.cascadestore.lsm.core.backgroundservice;

import java.util.concurrent.TimeUnit;

public interface BackgroundService {

  void start();

  void executeNow();

  void shutdown();

  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

  void shutdownNow();
}
