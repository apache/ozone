package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * This service monitors the flush time and SCMHADBTransactionBuffer, triggers
 * flush when last flush time elapsed too long and buffer size is not 0.
 * This provides the short-term visibility of SCMHADBTransactionBuffer where
 * flush opportunity is quantitative and not periodic.
 */
class SCMHADBTransactionBufferMonitorService extends BackgroundService {
  private static final Logger LOG = LoggerFactory.
      getLogger(SCMHADBTransactionBufferMonitorService.class);

  private final SCMHADBTransactionBufferImpl dbBuffer;
  private final long flushInterval;
  private static final int BUFFER_FLUSH_WORKER_POOL_SIZE = 1;


  SCMHADBTransactionBufferMonitorService(
      SCMHADBTransactionBufferImpl buffer,
      long serviceInterval,
      long serviceTimeout,
      TimeUnit unit,
      long flushTimeout) {
    super("SCMHADBTransactionBufferMonitorService",
        serviceInterval, unit, BUFFER_FLUSH_WORKER_POOL_SIZE, serviceTimeout);
    this.dbBuffer = buffer;
    this.flushInterval = flushTimeout;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new SCMHADBTransactionBufferFlushTask());
    return queue;
  }

  private class SCMHADBTransactionBufferFlushTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult.EmptyTaskResult call() throws Exception {
      long elapsedTime = Time.monotonicNow() - dbBuffer.getLastFlushTime();
      long size = dbBuffer.getBufferSize();
      if (elapsedTime > flushInterval && size > 0) {
        dbBuffer.flush();
        LOG.info("Last flush elapsed {} ms > timeout {} ms, and buffer size" +
            " is {}, trigger SCMHADBTransactionBuffer flush ",
            elapsedTime, flushInterval, size);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    @Override
    public int getPriority() {
      return 0;
    }
  }
}


