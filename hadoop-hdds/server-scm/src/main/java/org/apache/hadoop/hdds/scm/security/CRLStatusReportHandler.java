/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.security;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CRLStatusReport;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.CRLStatusReportFromDatanode;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.crl.CRLStatus;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Handles CRL Status Reports from datanode.
 */
public class CRLStatusReportHandler implements
    EventHandler<CRLStatusReportFromDatanode> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CRLStatusReportHandler.class);
  private CertificateStore certStore = null;
  private final boolean isSecurityEnabled;

  public CRLStatusReportHandler(CertificateStore certificateStore,
                                OzoneConfiguration conf) {
    isSecurityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    if (isSecurityEnabled) {
      Preconditions.checkNotNull(certificateStore);
      this.certStore = certificateStore;
    }
  }

  @Override
  public void onMessage(CRLStatusReportFromDatanode reportFromDatanode,
      EventPublisher publisher) {
    if (isSecurityEnabled) {
      Preconditions.checkNotNull(reportFromDatanode);
      DatanodeDetails dn = reportFromDatanode.getDatanodeDetails();
      Preconditions.checkNotNull(dn, "CRLStatusReport is "
          + "missing DatanodeDetails.");

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Processing CRL status report for dn: {}", dn);
      }
      CRLStatusReport crlStatusReport = reportFromDatanode.getReport();
      long receivedCRLId = crlStatusReport.getReceivedCrlId();
      List<Long> pendingCRLIds = crlStatusReport.getPendingCrlIdsList();

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Updating Processed CRL Id: {} and Pending CRL Ids: {} ",
            receivedCRLId,
            pendingCRLIds);
      }

      CRLStatus crlStatus = new CRLStatus(receivedCRLId, pendingCRLIds);
      certStore.setCRLStatusForDN(dn.getUuid(), crlStatus);

      // Todo: send command for new CRL
      // if crl > dn received crl id, then send a command to DN to process the
      // new CRL via heartbeat response.

    }
  }
}
