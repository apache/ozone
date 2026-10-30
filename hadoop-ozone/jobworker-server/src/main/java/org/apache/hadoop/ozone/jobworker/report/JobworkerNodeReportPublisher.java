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

package org.apache.hadoop.ozone.jobworker.report;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;

/**
 * Publishes JobworkerNodeReport, which will be sent to OM as part of heartbeat.
 */
public class JobworkerNodeReportPublisher
    extends JobworkerReportPublisher<JobworkerNodeReportProto> {

  private Long nodeReportInterval;

  @Override
  protected long getReportFrequency() {
    if (nodeReportInterval == null) {
      JobworkerConfiguration jwConf =
          getConf().getObject(JobworkerConfiguration.class);

      nodeReportInterval = jwConf.getNodeReportInterval().toMillis();
      long heartbeatFrequency = jwConf.getHeartbeatInterval().toMillis();

      Preconditions.checkState(
          heartbeatFrequency <= nodeReportInterval, String.format(
              "NodeReport interval %sms cannot be configured lower than heartbeat frequency %sms.",
              nodeReportInterval, heartbeatFrequency));
    }
    return nodeReportInterval;
  }

  @Override
  protected JobworkerNodeReportProto getReport() throws IOException {
    JobworkerNodeReportProto.Builder builder = JobworkerNodeReportProto.newBuilder();
    addStorageReports(builder);
    return builder.build();
  }

  /**
   * Add storage reports for each volume in the VolumeSet.
   *
   * @param builder The report builder
   */
  private void addStorageReports(JobworkerNodeReportProto.Builder builder) {
    List<JobworkerStorageReportProto> storageReports =
        getContext().getParent().getVolumeSet().getStorageReport();
    builder.addAllStorageReport(storageReports);
  }
}
