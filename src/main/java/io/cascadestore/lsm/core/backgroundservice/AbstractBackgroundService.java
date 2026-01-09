package io.cascadestore.lsm.core.backgroundservice;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBackgroundService implements BackgroundService {

  protected final Logger logger;
  protected final ScheduledExecutorService executorService;
  protected final String serviceName;

  protected AbstractBackgroundService(String serviceName) {
    this.serviceName = serviceName;
    this.logger = LoggerFactory.getLogger(this.getClass());

    this.executorService =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "CascadeStore-" + serviceName);
              thread.setDaemon(true);
              return thread;
            });
  }

  protected void scheduleTask(long initialDelaySeconds, long periodSeconds, TimeUnit timeUnit) {
    executorService.scheduleAtFixedRate(
        this::executeTask, initialDelaySeconds, periodSeconds, timeUnit);
    logger.info(
        serviceName
            + " service scheduled to run every "
            + periodSeconds
            + " "
            + timeUnit.toString().toLowerCase());
  }

  private void executeTask() {
    try {
      executeNow();
    } catch (Exception e) {
      logger.error("Error during " + serviceName + " execution", e);
    }
  }

  @Override
  public void executeNow() {
    try {
      logger.debug(serviceName + " service executing");
      doExecute();
      logger.debug(serviceName + " service completed");
    } catch (Exception e) {
      logger.error("Error during " + serviceName + " execution", e);
    }
  }

  protected abstract void doExecute();

  @Override
  public void shutdown() {
    logger.info(serviceName + " service shutting down");
    executorService.shutdown();

    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.warn(serviceName + " service did not terminate in time, forcing shutdown");
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      logger.warn(serviceName + " service shutdown interrupted, forcing shutdown");
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return executorService.awaitTermination(timeout, unit);
  }

  @Override
  public void shutdownNow() {
    logger.info(serviceName + " service shutting down now");
    executorService.shutdownNow();
  }
}
