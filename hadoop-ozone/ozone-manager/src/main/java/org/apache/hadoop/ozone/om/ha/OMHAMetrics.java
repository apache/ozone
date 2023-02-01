/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.ha;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class to maintain metrics and info related to OM HA.
 */
@Metrics(about = "OzoneManager HA Metrics", context = OzoneConsts.OZONE)
public final class OMHAMetrics implements MetricsSource {

  private enum OMHAMetricsInfo implements MetricsInfo {

    OzoneManagerHALeaderState("Leader active state " +
        "of OzoneManager node (1 leader, 0 follower)"),
    State("OM State (leader or follower)"),
    NodeId("OM node Id");

    private final String description;

    OMHAMetricsInfo(String description) {
      this.description = description;
    }

    @Override
    public String description() {
      return description;
    }
  }

  /**
   * Private nested class to hold
   * the values of OMHAMetricsInfo.
   */
  private static final class OMHAInfo {

    private long ozoneManagerHALeaderState;
    private String state;
    private String nodeId;

    OMHAInfo() {
      this.ozoneManagerHALeaderState = 0L;
      this.state = "";
      this.nodeId = "";
    }

    public long getOzoneManagerHALeaderState() {
      return ozoneManagerHALeaderState;
    }

    public void setOzoneManagerHALeaderState(long ozoneManagerHALeaderState) {
      this.ozoneManagerHALeaderState = ozoneManagerHALeaderState;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }
  }

  public static final String SOURCE_NAME =
      OMHAMetrics.class.getSimpleName();
  private final OMHAInfo omhaInfo = new OMHAInfo();
  private MetricsRegistry metricsRegistry;

  private String currNodeId;
  private String leaderId;

  public OMHAMetrics(String currNodeId, String leaderId) {
    this.currNodeId = currNodeId;
    this.leaderId = leaderId;
    this.metricsRegistry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and return OMHAMetrics instance.
   * @return OMHAMetrics
   */
  public static synchronized OMHAMetrics create(
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

    if (currNodeId.equals(leaderId)) {
      omhaInfo.setNodeId(currNodeId);
      omhaInfo.setState("leader");
      omhaInfo.setOzoneManagerHALeaderState(1);

      recordBuilder
          .tag(OMHAMetricsInfo.NodeId, currNodeId)
          .tag(OMHAMetricsInfo.State, "leader")
          .addGauge(OMHAMetricsInfo.OzoneManagerHALeaderState, 1);
    } else {
      omhaInfo.setNodeId(currNodeId);
      omhaInfo.setState("follower");
      omhaInfo.setOzoneManagerHALeaderState(0);

      recordBuilder
          .tag(OMHAMetricsInfo.NodeId, currNodeId)
          .tag(OMHAMetricsInfo.State, "follower")
          .addGauge(OMHAMetricsInfo.OzoneManagerHALeaderState, 0);
    }
    recordBuilder.endRecord();
  }

  @VisibleForTesting
  public String getOmhaInfoNodeId() {
    return omhaInfo.getNodeId();
  }

  @VisibleForTesting
  public String getOmhaInfoState() {
    return omhaInfo.getState();
  }

  @VisibleForTesting
  public long getOmhaInfoOzoneManagerHALeaderState() {
    return omhaInfo.getOzoneManagerHALeaderState();
  }
}
