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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * SCM HA metrics.
 */
@Metrics(about = "SCM HA metrics", context = OzoneConsts.OZONE)
public final class SCMHAMetrics implements MetricsSource {

  private static final String SOURCE_NAME = SCMHAMetrics.class.getSimpleName();
  private final SCMHAMetricsInfo scmHAMetricsInfo = new SCMHAMetricsInfo();
  private final String currNodeId;
  private final String leaderId;
  private final SCMHADBTransactionBuffer transactionBuffer;

  private SCMHAMetrics(String currNodeId, String leaderId,
      SCMHADBTransactionBuffer transactionBuffer) {
    this.currNodeId = currNodeId;
    this.leaderId = leaderId;
    this.transactionBuffer = transactionBuffer;
  }

  /**
   * Creates and returns SCMHAMetrics instance.
   * @return SCMHAMetrics
   */
  public static SCMHAMetrics create(String nodeId, String leaderId) {
    return create(nodeId, leaderId, null);
  }

  /**
   * Creates and returns SCMHAMetrics instance.
   * @return SCMHAMetrics
   */
  public static SCMHAMetrics create(String nodeId, String leaderId,
      SCMHADBTransactionBuffer transactionBuffer) {
    SCMHAMetrics metrics = new SCMHAMetrics(nodeId, leaderId, transactionBuffer);
    return DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "SCM HA metrics", metrics);
  }

  /**
   * Unregisters the metrics instance.
   */
  public static void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    // Check current node state (1 leader, 0 follower)
    int state = currNodeId.equals(leaderId) ? 1 : 0;
    long latestSnapshotIndex = transactionBuffer == null ? -1
        : transactionBuffer.getLatestSnapshotIndex();
    long lastSnapshotTimeMs = transactionBuffer == null ? -1
        : transactionBuffer.getLastSnapshotTimeMs();
    scmHAMetricsInfo.setNodeId(currNodeId);
    scmHAMetricsInfo.setScmHALeaderState(state);
    scmHAMetricsInfo.setScmHALastTransactionInfoIndex(latestSnapshotIndex);
    scmHAMetricsInfo.setScmHALastFlushTimeMs(lastSnapshotTimeMs);

    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);
    recordBuilder
        .tag(SCMHAMetricsInfo.NODE_ID, currNodeId)
        .addGauge(SCMHAMetricsInfo.SCM_MANAGER_HA_LEADER_STATE, state)
        .addGauge(SCMHAMetricsInfo.SCM_MANAGER_HA_LAST_TRANSACTION_INFO_INDEX,
            latestSnapshotIndex)
        .addGauge(SCMHAMetricsInfo.SCM_MANAGER_HA_LAST_FLUSH_TIME_MS, lastSnapshotTimeMs);
    recordBuilder.endRecord();
  }

  @VisibleForTesting
  public String getSCMHAMetricsInfoNodeId() {
    return scmHAMetricsInfo.getNodeId();
  }

  @VisibleForTesting
  public int getSCMHAMetricsInfoLeaderState() {
    return scmHAMetricsInfo.getScmHALeaderState();
  }

  @VisibleForTesting
  public long getSCMHAMetricsInfoLastTransactionInfoIndex() {
    return scmHAMetricsInfo.getScmHALastTransactionInfoIndex();
  }

  @VisibleForTesting
  public long getSCMHAMetricsInfoLastFlushTimeMs() {
    return scmHAMetricsInfo.getScmHALastFlushTimeMs();
  }

  /**
   * Metrics value holder.
   */
  private static final class SCMHAMetricsInfo {

    private static final MetricsInfo SCM_MANAGER_HA_LEADER_STATE =
        Interns.info("SCMHALeaderState", "Leader active " +
            "state of SCM node (1 leader, 0 follower");
    private static final MetricsInfo SCM_MANAGER_HA_LAST_TRANSACTION_INFO_INDEX =
        Interns.info("SCMHALastTransactionInfoIndex",
            "Latest flushed transactionInfo index in SCM DB");
    private static final MetricsInfo SCM_MANAGER_HA_LAST_FLUSH_TIME_MS =
        Interns.info("SCMHALastFlushTimeMs",
            "Last SCM DB flush timestamp in milliseconds");
    private static final MetricsInfo NODE_ID = Interns.info("NodeId",
        "SCM node Id");
    private int scmHALeaderState;
    private long scmHALastTransactionInfoIndex;
    private long scmHALastFlushTimeMs;
    private String nodeId;

    public int getScmHALeaderState() {
      return scmHALeaderState;
    }

    public void setScmHALeaderState(int scmHALeaderState) {
      this.scmHALeaderState = scmHALeaderState;
    }

    public long getScmHALastTransactionInfoIndex() {
      return scmHALastTransactionInfoIndex;
    }

    public void setScmHALastTransactionInfoIndex(
        long scmHALastTransactionInfoIndex) {
      this.scmHALastTransactionInfoIndex = scmHALastTransactionInfoIndex;
    }

    public long getScmHALastFlushTimeMs() {
      return scmHALastFlushTimeMs;
    }

    public void setScmHALastFlushTimeMs(long scmHALastFlushTimeMs) {
      this.scmHALastFlushTimeMs = scmHALastFlushTimeMs;
    }

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }
  }
}
