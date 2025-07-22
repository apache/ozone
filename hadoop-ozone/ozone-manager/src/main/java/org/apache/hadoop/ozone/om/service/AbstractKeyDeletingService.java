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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.ratis.protocol.ClientId;

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
  private final BootstrapStateHandler.Lock lock =
      new BootstrapStateHandler.Lock();

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
  }

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
