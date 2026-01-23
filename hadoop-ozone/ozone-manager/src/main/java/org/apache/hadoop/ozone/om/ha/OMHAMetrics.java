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

package org.apache.hadoop.ozone.om.ha;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class to maintain metrics and info related to OM HA.
 */
@Metrics(about = "OzoneManager HA Metrics", context = OzoneConsts.OZONE)
public final class OMHAMetrics implements MetricsSource {

  public static final String SOURCE_NAME = OMHAMetrics.class.getSimpleName();
  private final OMHAMetricsInfo omhaMetricsInfo = new OMHAMetricsInfo();
  private MetricsRegistry metricsRegistry;

  private String currNodeId;
  private String leaderId;

  /**
   * Private nested class to hold the values
   * of MetricsInfo for OMHAMetrics.
   */
  private static final class OMHAMetricsInfo {

    private static final MetricsInfo OZONE_MANAGER_HA_LEADER_STATE =
        Interns.info("OzoneManagerHALeaderState",
            "Leader active state of OzoneManager node (1 leader, 0 follower)");

    private static final MetricsInfo NODE_ID =
        Interns.info("NodeId", "OM node Id");

    private int ozoneManagerHALeaderState;
    private String nodeId;

    OMHAMetricsInfo() {
      this.ozoneManagerHALeaderState = 0;
      this.nodeId = "";
    }

    public int getOzoneManagerHALeaderState() {
      return ozoneManagerHALeaderState;
    }

    public void setOzoneManagerHALeaderState(int ozoneManagerHALeaderState) {
      this.ozoneManagerHALeaderState = ozoneManagerHALeaderState;
    }

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }
  }

  private OMHAMetrics(String currNodeId, String leaderId) {
    this.currNodeId = currNodeId;
    this.leaderId = leaderId;
    this.metricsRegistry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and return OMHAMetrics instance.
   * @return OMHAMetrics
   */
  public static OMHAMetrics create(
      String nodeId, String leaderId) {
    OMHAMetrics metrics = new OMHAMetrics(nodeId, leaderId);
    return DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "Metrics for OM HA", metrics);
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {

    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    // Check current node state (1 leader, 0 follower)
    int state = currNodeId.equals(leaderId) ? 1 : 0;
    omhaMetricsInfo.setNodeId(currNodeId);
    omhaMetricsInfo.setOzoneManagerHALeaderState(state);

    recordBuilder
        .tag(OMHAMetricsInfo.NODE_ID, currNodeId)
        .addGauge(OMHAMetricsInfo.OZONE_MANAGER_HA_LEADER_STATE, state);

    recordBuilder.endRecord();
  }

  @VisibleForTesting
  public String getOmhaInfoNodeId() {
    return omhaMetricsInfo.getNodeId();
  }

  @VisibleForTesting
  public int getOmhaInfoOzoneManagerHALeaderState() {
    return omhaMetricsInfo.getOzoneManagerHALeaderState();
  }
}
