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

package org.apache.hadoop.ozone.om.multiraft;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.om.OzoneManager;

/**
 * Class to maintain metrics related to OM HA Multi-Raft.
 */
@Metrics(
    about = "Ozone Manager HA Multi-Raft Metrics",
    context = "ozone")
public class OMHAMultiRaftMetrics implements MetricsSource  {

  private final OzoneManager ozoneManager;

  private static final String SOURCE_NAME = OMHAMultiRaftMetrics.class.getSimpleName();

  public OMHAMultiRaftMetrics(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  public static OMHAMultiRaftMetrics create(OzoneManager ozoneManager) {
    OMHAMultiRaftMetrics omhaMultiRaftMetrics = new OMHAMultiRaftMetrics(ozoneManager);
    return DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "Metrics for OM HA", omhaMultiRaftMetrics);
  }

  public static void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {
    metricsCollector.addRecord(OMHAMultiRaftMetrics.class.getSimpleName())
        .setContext("ozone")
        .addGauge(Interns.info("RaftGroupsCount", "OM Raft Groups Count"), getOmRaftGroupsCount())
        .endRecord();
    metricsCollector.addRecord(OMHAMultiRaftMetrics.class.getSimpleName())
        .setContext("ozone")
        .addGauge(Interns.info("RaftGroupsExpectedCount", "OM Raft Groups Expected Count"),
            getRaftGroupsExpectedCount())
        .endRecord();
    metricsCollector.addRecord(OMHAMultiRaftMetrics.class.getSimpleName())
        .setContext("ozone")
        .addGauge(Interns.info("OMInSafeMode", "Ozone Manager is in safe mode"),
            getIsOzoneManagerInSafeMode())
        .endRecord();
  }

  @VisibleForTesting
  public int getIsOzoneManagerInSafeMode() {
    return ozoneManager.getSafeModeManager().isInSafeMode() ? 1 : 0;
  }

  @VisibleForTesting
  public int getOmRaftGroupsCount() {
    return ozoneManager.getOmRaftGroups().size();
  }

  @VisibleForTesting
  public int getRaftGroupsExpectedCount() {
    return ozoneManager.getConfiguration().getPositiveIntOrDefault(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT) + 1;
  }
}
