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
import org.apache.ratis.protocol.RaftGroupId;

import java.util.HashMap;
import java.util.Map;

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

    private static final MetricsInfo RAFT_GROUP_ID =
        Interns.info("RaftGroupId", "Raft Group Id");

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

  private Map<RaftGroupId, String> raftGroupsLeaders = new HashMap<>();
  private RaftGroupId mainRaftGroupId;

  private OMHAMetrics(String currNodeId, String leaderId, RaftGroupId raftGroupId) {
    this.currNodeId = currNodeId;
    raftGroupsLeaders.put(raftGroupId, leaderId);
    this.leaderId = leaderId;
    this.mainRaftGroupId = raftGroupId;
    this.metricsRegistry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and return OMHAMetrics instance.
   * @return OMHAMetrics
   */
  public static OMHAMetrics create(
      String nodeId, String leaderId, RaftGroupId raftGroupId) {
    OMHAMetrics metrics = new OMHAMetrics(nodeId, leaderId, raftGroupId);
    return DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "Metrics for OM HA", metrics);
  }

  public void resetMainGroup() {
    this.leaderId = null;
    this.mainRaftGroupId = null;
  }

  public OMHAMetrics defineRaftGroupLeader(
      RaftGroupId raftGroupId, String groupLeaderId, boolean isMainRaftGroup) {
    // Update the raft group leader map with the new leader
    raftGroupsLeaders.put(raftGroupId, groupLeaderId);

    if (isMainRaftGroup) {
      this.leaderId = groupLeaderId;
      this.mainRaftGroupId = raftGroupId;
    }
    return this;
  }

  public void deleteRaftGroup(RaftGroupId raftGroupId) {
    raftGroupsLeaders.remove(raftGroupId);
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

    MetricsInfo infoBucketRaftGroup = Interns.info("OzoneManagerBucketRaftGroupLeaderState",
        "OM Bucket Raft Group Leader State");
    MetricsInfo infoMainRaftGroup = Interns.info("OzoneManagerRaftGroupLeaderState",
        "OM Main Raft Group Leader State");
    for (int i = 0; i < raftGroupsLeaders.size(); i++) {
      recordBuilder = collector.addRecord(SOURCE_NAME);
      RaftGroupId raftGroupId = (RaftGroupId) raftGroupsLeaders.keySet().toArray()[i];
      String leader = raftGroupsLeaders.get(raftGroupId);
      int raftGroupLeaderState = leader.equals(currNodeId) ? 1 : 0;
      recordBuilder = recordBuilder
          .tag(OMHAMetricsInfo.NODE_ID, currNodeId)
          .tag(OMHAMetricsInfo.RAFT_GROUP_ID, raftGroupId.toString());
      if (raftGroupId.equals(mainRaftGroupId)) {
        recordBuilder.addGauge(infoMainRaftGroup, raftGroupLeaderState);
      } else {
        recordBuilder.addGauge(infoBucketRaftGroup, raftGroupLeaderState);
      }
      recordBuilder.endRecord();
    }

  }

  @VisibleForTesting
  public String getOmhaInfoNodeId() {
    return omhaMetricsInfo.getNodeId();
  }

  @VisibleForTesting
  public int getOmhaInfoOzoneManagerHALeaderState() {
    return omhaMetricsInfo.getOzoneManagerHALeaderState();
  }

  @VisibleForTesting
  public Map<RaftGroupId, String> getOmHaInfoRaftGroupsLeaders() {
    return raftGroupsLeaders;
  }
}
