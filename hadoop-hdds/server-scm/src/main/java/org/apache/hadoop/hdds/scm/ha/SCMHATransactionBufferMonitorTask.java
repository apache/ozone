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

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background service running in SCM to check and flush the HA Transaction
 * buffer.
 */
public class SCMHATransactionBufferMonitorTask implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMHATransactionBufferMonitorTask.class);
  private final SCMRatisServer server;
  private final SCMHADBTransactionBuffer transactionBuffer;
  private final long flushInterval;

  /**
   * SCMService related variables.
   */
  public SCMHATransactionBufferMonitorTask(
      SCMHADBTransactionBuffer transactionBuffer,
      SCMRatisServer server, long flushInterval) {
    this.flushInterval = flushInterval;
    this.transactionBuffer = transactionBuffer;
    this.server = server;
  }

  @Override
  public void run() {
    if (transactionBuffer.shouldFlush(flushInterval)) {
      LOG.debug("Running TransactionFlushTask");
      // set latest snapshot to null for force snapshot
      // the value will be reset again when snapshot is taken
      final SnapshotInfo lastSnapshot = transactionBuffer
          .getLatestSnapshotRef().getAndSet(null);
      try {
        server.triggerSnapshot();
      } catch (IOException e) {
        LOG.error("Snapshot request is failed", e);
      } finally {
        // under failure case, if unable to take snapshot, its value
        // is reset to previous known value
        transactionBuffer.getLatestSnapshotRef().compareAndSet(
            null, lastSnapshot);
      }
    }
  }
}
