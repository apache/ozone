/*
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
import org.apache.hadoop.hdds.datanode.metadata.DatanodeCRLStore;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CRLStatusReport;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.HddsServerUtil;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CRL_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CRL_STATUS_REPORT_INTERVAL_DEFAULT;

/**
 * Publishes CRLStatusReport which will be sent to SCM as part of heartbeat.
 * CRLStatusReport consist of the following information:
 *   - receivedCRLId : The latest processed CRL Sequence ID.
 *   - pendingCRLIds : The list of CRL IDs that are still pending in the
 *   queue to be processed in the future.
 */
public class CRLStatusReportPublisher extends
    ReportPublisher<CRLStatusReport> {

  private Long crlStatusReportInterval = null;

  @Override
  protected long getReportFrequency() {
    if (crlStatusReportInterval == null) {
      crlStatusReportInterval = getConf().getTimeDuration(
          HDDS_CRL_STATUS_REPORT_INTERVAL,
          HDDS_CRL_STATUS_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);

      long heartbeatFrequency = HddsServerUtil.getScmHeartbeatInterval(
          getConf());

      Preconditions.checkState(
          heartbeatFrequency <= crlStatusReportInterval,
          HDDS_CRL_STATUS_REPORT_INTERVAL +
              " cannot be configured lower than heartbeat frequency.");
    }
    return crlStatusReportInterval;
  }

  @Override
  protected CRLStatusReport getReport() throws IOException {

    CRLStatusReport.Builder builder = CRLStatusReport.newBuilder();

    DatanodeCRLStore dnCRLStore = this.getContext().getParent().getDnCRLStore();

    builder.setReceivedCrlId(dnCRLStore.getLatestCRLSequenceID());
    if (dnCRLStore.getPendingCRLs().size() > 0) {
      List<Long> pendingCRLIds =
          dnCRLStore.getPendingCRLs().stream().map(CRLInfo::getCrlSequenceID)
              .collect(Collectors.toList());
      builder.addAllPendingCrlIds(pendingCRLIds);
    }

    return builder.build();
  }
}
