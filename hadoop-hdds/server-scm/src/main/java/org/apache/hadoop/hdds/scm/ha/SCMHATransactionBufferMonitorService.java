/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL_DEFAULT;

/**
 * A background service running in SCM to check and flush the HA Transaction
 * buffer.
 */
public class SCMHATransactionBufferMonitorService extends BackgroundService
    implements SCMService {
  public static final Logger LOG =
      LoggerFactory.getLogger(SCMHATransactionBufferMonitorService.class);
  private static final int SERVICE_CORE_POOL_SIZE = 1;
  
  private SCMRatisServer server;
  private final SCMContext scmContext;
  private SCMHADBTransactionBuffer transactionBuffer;
  private long flushInterval = 0;

  /**
   * SCMService related variables.
   */
  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

  @SuppressWarnings("parameternumber")
  public SCMHATransactionBufferMonitorService(
      SCMHADBTransactionBuffer transactionBuffer,
      SCMContext scmContext,
      ConfigurationSource conf,
      SCMRatisServer server) {
    super("SCMHATransactionBufferMonitorService",
        conf.getTimeDuration(OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
            OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS, SERVICE_CORE_POOL_SIZE,
        conf.getTimeDuration(OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
            OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS));
    this.flushInterval = conf.getTimeDuration(
        OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
        OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.scmContext = scmContext;
    this.transactionBuffer = transactionBuffer;
    this.server = server;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new TransactionFlushTask());
    return queue;
  }

  private class TransactionFlushTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 1;
    }

    @Override
    public EmptyTaskResult call() throws Exception {
      if (!shouldRun()) {
        return EmptyTaskResult.newResult();
      }

      if (transactionBuffer.shouldFlush(flushInterval)) {
        LOG.debug("Running TransactionFlushTask");
        SCMRatisResponse reply = server.submitSnapshotRequest();
        if (!reply.isSuccess()) {
          LOG.error("Submit snapshot request failed", reply.getException());
        }
      }

      return EmptyTaskResult.newResult();
    }
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      if (scmContext.isLeader() && !scmContext.isInSafeMode() &&
          serviceStatus != ServiceStatus.RUNNING) {
        serviceStatus = ServiceStatus.RUNNING;
      } else {
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      return serviceStatus == ServiceStatus.RUNNING;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public String getServiceName() {
    return SCMHATransactionBufferMonitorService.class.getSimpleName();
  }

  @Override
  public void stop() {
    shutdown();
  }
}
