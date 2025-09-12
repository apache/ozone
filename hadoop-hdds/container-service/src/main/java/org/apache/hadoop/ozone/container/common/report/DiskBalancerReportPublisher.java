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

package org.apache.hadoop.ozone.container.common.report;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DISK_BALANCER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DISK_BALANCER_REPORT_INTERVAL_DEFAULT;

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerInfo;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerService;

/**
 * Publishes DiskBalancer report which will be sent to SCM as part of heartbeat.
 * Report is only published is lastPublishedReport is different from currentReport,
 * when balancer is in stopped state.
 * But when balancer is running or paused by node state, report is actively sent to SCM.
 * DiskBalancer Report consist of the following information:
 *   - isBalancerRunning
 *   - balancedBytes
 *   - DiskBalancerConfiguration
 *   - successCount
 *   - failureCount
 *   - bytesToMove
 */
public class DiskBalancerReportPublisher extends
    ReportPublisher<DiskBalancerReportProto> {

  private Long diskBalancerReportInterval = null;

  // Cache the last published report to detect changes when balancer is stopped
  private DiskBalancerReportProto lastPublishedReport = null;

  @Override
  protected long getReportFrequency() {
    if (diskBalancerReportInterval == null) {
      diskBalancerReportInterval = getConf().getTimeDuration(
          HDDS_DISK_BALANCER_REPORT_INTERVAL,
          HDDS_DISK_BALANCER_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);

      long heartbeatFrequency = HddsServerUtil.getScmHeartbeatInterval(
          getConf());

      Preconditions.checkState(
          heartbeatFrequency <= diskBalancerReportInterval,
              HDDS_DISK_BALANCER_REPORT_INTERVAL +
              " cannot be configured lower than heartbeat frequency " +
                  heartbeatFrequency + ".");
    }
    return diskBalancerReportInterval;
  }

  @Override
  protected DiskBalancerReportProto getReport() {
    DiskBalancerInfo info = getContext().getParent().getContainer().getDiskBalancerInfo();
    if (info == null) {
      return null;
    }

    DiskBalancerReportProto currentReport = info.toDiskBalancerReportProto();

    // Always publish if DiskBalancer state is running or paused by node state
    if (info.getOperationalState() == DiskBalancerService.DiskBalancerOperationalState.RUNNING
        || info.getOperationalState() == DiskBalancerService.DiskBalancerOperationalState.PAUSED_BY_NODE_STATE) {
      lastPublishedReport = currentReport;
      return currentReport;
    }

    // DiskBalancer is in stopped state.
    // Publish if there is a change in the report since last publish
    if (!currentReport.equals(lastPublishedReport)) {
      lastPublishedReport = currentReport;
      return currentReport;
    }
    // the balancer is stopped and lastPublishedReport
    // is unchanged, so don't send any report.
    return null;
  }
}
