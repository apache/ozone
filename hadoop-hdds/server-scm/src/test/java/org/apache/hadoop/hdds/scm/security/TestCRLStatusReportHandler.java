/*
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.security;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CRLStatusReport;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.server.SCMCertStore;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.CRLStatusReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.crl.CRLStatus;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

/**
 * Test for the CRL Status Report Handler.
 */
public class TestCRLStatusReportHandler implements EventPublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCRLStatusReportHandler.class);
  private CRLStatusReportHandler crlStatusReportHandler;
  private CertificateStore certificateStore;
  private SCMMetadataStore scmMetadataStore;

  @BeforeEach
  public void init(@TempDir Path tempDir) throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();

    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.toAbsolutePath().toString());
    config.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);

    SCMStorageConfig storageConfig = Mockito.mock(SCMStorageConfig.class);
    Mockito.when(storageConfig.getClusterID()).thenReturn("cluster1");
    scmMetadataStore = new SCMMetadataStoreImpl(config);
    certificateStore = new SCMCertStore.Builder()
        .setRatisServer(null)
        .setMetadaStore(scmMetadataStore)
        .build();
    crlStatusReportHandler =
        new CRLStatusReportHandler(certificateStore, config);
  }

  @AfterEach
  public void destroyDbStore() throws Exception {
    if (scmMetadataStore.getStore() != null) {
      scmMetadataStore.getStore().close();
      scmMetadataStore = null;
    }
  }

  @Test
  public void testCRLStatusReport() {
    DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    List<Long> pendingCRLIds1 = new ArrayList<>();
    List<Long> pendingCRLIds2 = new ArrayList<>();
    pendingCRLIds1.add(3L);
    pendingCRLIds1.add(4L);
    pendingCRLIds2.add(1L);
    CRLStatusReportFromDatanode reportFromDatanode1 =
        getCRLStatusReport(dn1, pendingCRLIds1, 5L);
    CRLStatusReportFromDatanode reportFromDatanode2 =
        getCRLStatusReport(dn2, pendingCRLIds2, 2L);
    crlStatusReportHandler.onMessage(reportFromDatanode1, this);
    CRLStatus crlStatus = certificateStore.getCRLStatusForDN(dn1.getUuid());
    Assertions.assertTrue(
        crlStatus.getPendingCRLIds().containsAll(pendingCRLIds1));
    Assertions.assertEquals(5L, crlStatus.getReceivedCRLId());

    pendingCRLIds1.remove(0);
    reportFromDatanode1 = getCRLStatusReport(dn1, pendingCRLIds1, 6L);
    crlStatusReportHandler.onMessage(reportFromDatanode1, this);
    crlStatus = certificateStore.getCRLStatusForDN(dn1.getUuid());
    Assertions.assertEquals(1, crlStatus.getPendingCRLIds().size());
    Assertions.assertEquals(4L,
        crlStatus.getPendingCRLIds().get(0).longValue());
    Assertions.assertEquals(6L, crlStatus.getReceivedCRLId());

    crlStatusReportHandler.onMessage(reportFromDatanode2, this);
    crlStatus = certificateStore.getCRLStatusForDN(dn2.getUuid());
    Assertions.assertTrue(
        crlStatus.getPendingCRLIds().containsAll(pendingCRLIds2));
    Assertions.assertEquals(2L, crlStatus.getReceivedCRLId());
  }

  private CRLStatusReportFromDatanode getCRLStatusReport(
      DatanodeDetails dn,
      List<Long> pendingCRLIds,
      long receivedCRLId) {
    CRLStatusReport crlStatusReportProto =
        HddsTestUtils.createCRLStatusReport(pendingCRLIds, receivedCRLId);
    return new CRLStatusReportFromDatanode(dn, crlStatusReportProto);
  }

  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {
    LOG.info("Event is published: {}", payload);
  }
}
