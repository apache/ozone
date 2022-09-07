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

package org.apache.hadoop.ozone.container.common.report;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.utils.HddsServerUtil;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DISK_BALANCER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DISK_BALANCER_REPORT_INTERVAL_DEFAULT;


/**
 * Publishes DiskBalancer report which will be sent to SCM as part of heartbeat.
 * DiskBalancer Report consist of the following information:
 *   - isBalancerRunning
 *   - balancedBytes
 *   - DiskBalancerConfiguration
 */
public class DiskBalancerReportPublisher extends
    ReportPublisher<DiskBalancerReportProto> {

  private Long diskBalancerReportInterval = null;

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
    return getContext().getParent().getContainer().getDiskBalancerReport();
  }
}
