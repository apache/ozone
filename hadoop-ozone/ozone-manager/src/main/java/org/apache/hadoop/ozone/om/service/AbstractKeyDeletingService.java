/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.BOOTSTRAP_LOCK;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * Abstracts common code from KeyDeletingService and DirectoryDeletingService
 * which is now used by SnapshotDeletingService as well.
 */
public abstract class AbstractKeyDeletingService extends BackgroundService
    implements BootstrapStateHandler {

  private final OzoneManager ozoneManager;
  private final DeletingServiceMetrics metrics;
  private final OMPerformanceMetrics perfMetrics;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong runCount;
  private final AtomicLong callId;
  private final AtomicBoolean suspended;
  private final BootstrapStateHandler.Lock lock;

  public AbstractKeyDeletingService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout,
      OzoneManager ozoneManager) {
    super(serviceName, interval, unit, threadPoolSize, serviceTimeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.runCount = new AtomicLong(0);
    this.metrics = ozoneManager.getDeletionMetrics();
    this.perfMetrics = ozoneManager.getPerfMetrics();
    this.callId = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
    IOzoneManagerLock ozoneManagerLock = ozoneManager.getMetadataManager().getLock();
    Function<Boolean, UncheckedAutoCloseable> lockSupplier = (readLock) ->
        ozoneManagerLock.acquireLock(BOOTSTRAP_LOCK, getServiceName(), readLock);
    this.lock = new BootstrapStateHandler.Lock(lockSupplier);
  }

  @Override
  public abstract DeletingServiceTaskQueue getTasks();

  protected OMResponse submitRequest(OMRequest omRequest) throws ServiceException {
    return OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, callId.incrementAndGet());
  }

  final boolean shouldRun() {
    if (getOzoneManager() == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return !suspended.get() && getOzoneManager().isLeaderReady();
  }

  boolean isPreviousPurgeTransactionFlushed() throws IOException {
    TransactionInfo lastAOSTransactionId = metrics.getLastAOSTransactionInfo();
    TransactionInfo flushedTransactionId = TransactionInfo.readTransactionInfo(
        getOzoneManager().getMetadataManager());
    if (flushedTransactionId != null && lastAOSTransactionId.compareTo(flushedTransactionId) > 0) {
      LOG.info("Skipping AOS processing since changes to deleted space of AOS have not been flushed to disk " +
              "last Purge Transaction: {}, Flushed Disk Transaction: {}", lastAOSTransactionId,
          flushedTransactionId);
      return false;
    }
    return true;
  }

  private static final class BackgroundDeleteTask implements BackgroundTask {
    private final BootstrapStateHandler.Lock bootstrapLock;
    private final BackgroundTask task;

    private BackgroundDeleteTask(BootstrapStateHandler.Lock bootstrapLock, BackgroundTask task) {
      this.bootstrapLock = bootstrapLock;
      this.task = task;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      try (UncheckedAutoCloseable readLock = bootstrapLock.acquireReadLock()) {
        return task.call();
      }
    }

    @Override
    public int getPriority() {
      return task.getPriority();
    }
  }

  /**
   * A specialized implementation of {@link BackgroundTaskQueue} that modifies
   * the behavior of added tasks to utilize a read lock during execution.
   *
   * This class ensures that every {@link BackgroundTask} added to the queue
   * is wrapped such that its execution acquires a read lock via
   * {@code getBootstrapStateLock().acquireReadLock()} before performing any
   * operations. The lock is automatically released upon task completion or
   * exception, ensuring safe concurrent execution of tasks within the service when running along with bootstrap flow.
   */
  public class DeletingServiceTaskQueue extends BackgroundTaskQueue {
    @Override
    public synchronized void add(BackgroundTask task) {
      super.add(new BackgroundDeleteTask(lock, task));
    }
  }

  /**
   * Suspend the service.
   */
  @VisibleForTesting
  public void suspend() throws ExecutionException, InterruptedException {
    suspended.set(true);
    getFuture().get();
  }

  /**
   * Resume the service if suspended.
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  protected boolean isBufferLimitCrossed(
      int maxLimit, int cLimit, int increment) {
    return cLimit + increment >= maxLimit;
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  ClientId getClientId() {
    return clientId;
  }

  DeletingServiceMetrics getMetrics() {
    return metrics;
  }

  OMPerformanceMetrics getPerfMetrics() {
    return perfMetrics;
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public AtomicLong getRunCount() {
    return runCount;
  }

  public AtomicLong getCallId() {
    return callId;
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  /**
   * Submits SetSnapsnapshotPropertyRequest to OM.
   * @param setSnapshotPropertyRequests request to be sent to OM
   */
  protected void submitSetSnapshotRequests(
      List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests) {
    if (setSnapshotPropertyRequests.isEmpty()) {
      return;
    }
    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetSnapshotProperty)
        .addAllSetSnapshotPropertyRequests(setSnapshotPropertyRequests)
        .setClientId(clientId.toString())
        .build();
    try {
      submitRequest(omRequest);
    } catch (ServiceException e) {
      LOG.error("Failed to submit set snapshot property request", e);
    }
  }
}
