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

package org.apache.hadoop.hdds.scm.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.util.Collections;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetSCMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Type;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the leaderless-primary bypass added to
 * {@link SCMSecurityProtocolServerSideTranslatorPB#submitRequest}: a non-leader SCM that hosts
 * the primary root CA and has no known Ratis leader may still service a non-renewal
 * {@code GetSCMCertificate} request, while every other combination keeps rejecting the request
 * exactly as before (HDDS-8286 behaviour).
 */
class TestSCMSecurityProtocolServerSideTranslatorPB {

  private static final String SCM_NODE_ID = "scm-2";

  private SCMSecurityProtocol impl;
  private SCMRatisServer ratisServer;
  private SCMSecurityProtocolServerSideTranslatorPB translator;

  private void withRatisPeer(String peerId) {
    when(ratisServer.getDivision().getRaftConf().getCurrentPeers()).thenReturn(
        Collections.singletonList(RaftPeer.newBuilder().setId(peerId)
            .setAddress("scm2.example.com:9894").build()));
  }

  @BeforeEach
  void setup() throws Exception {
    impl = mock(SCMSecurityProtocol.class);
    StorageContainerManager scm = mock(StorageContainerManager.class);
    SCMHAManager haManager = mock(SCMHAManager.class);
    ratisServer = mock(SCMRatisServer.class, RETURNS_DEEP_STUBS);
    NotLeaderException nle = mock(NotLeaderException.class);

    when(scm.checkLeader()).thenReturn(false);
    when(scm.getScmHAManager()).thenReturn(haManager);
    when(haManager.getRatisServer()).thenReturn(ratisServer);
    when(ratisServer.triggerNotLeaderException()).thenReturn(nle);
    when(nle.getSuggestedLeader()).thenReturn(null);

    // The requesting SCM is already a member of the existing Ratis group.
    withRatisPeer(SCM_NODE_ID);

    when(scm.getRootCertificateServer()).thenReturn(mock(CertificateServer.class));

    SCMStorageConfig storageConfig = mock(SCMStorageConfig.class);
    when(storageConfig.isSCMHAEnabled()).thenReturn(true);
    when(storageConfig.getPrimaryScmNodeId()).thenReturn("scm-1");
    when(storageConfig.getScmId()).thenReturn("scm-1");
    when(scm.getScmStorageConfig()).thenReturn(storageConfig);

    when(scm.getSecurityProtocolRpcPort()).thenReturn("9961");
    when(scm.getScmId()).thenReturn("scm-1");
    when(scm.getHostname()).thenReturn("scm1.example.com");

    ProtocolMessageMetrics<Type> metrics = new ProtocolMessageMetrics<>(
        "ScmSecurityProtocol", "SCM Security protocol metrics", Type.class);
    translator = new SCMSecurityProtocolServerSideTranslatorPB(impl, scm, metrics);
  }

  private static SCMGetSCMCertRequestProto.Builder scmCertRequestBuilder() {
    ScmNodeDetailsProto scmDetails = ScmNodeDetailsProto.newBuilder()
        .setScmNodeId(SCM_NODE_ID)
        .setClusterId("cluster-1")
        .setHostName("scm2.example.com")
        .build();
    return SCMGetSCMCertRequestProto.newBuilder()
        .setScmDetails(scmDetails)
        .setCSR("csr");
  }

  private static SCMSecurityRequest wrap(SCMGetSCMCertRequestProto.Builder certRequestBuilder) {
    return SCMSecurityRequest.newBuilder()
        .setCmdType(Type.GetSCMCertificate)
        .setTraceID("trace-1")
        .setGetSCMCertificateRequest(certRequestBuilder.build())
        .build();
  }

  @Test
  void bypassesRejectionForLeaderlessPrimaryScmCertRequest() throws Exception {
    when(impl.getSCMCertificate(any(), eq("csr"), eq(false))).thenReturn("scm-cert-pem");
    when(impl.getRootCACertificate()).thenReturn("root-ca-pem");

    SCMSecurityRequest request = wrap(scmCertRequestBuilder());

    assertDoesNotThrow(() -> translator.submitRequest(null, request));

    verify(impl).getSCMCertificate(any(), eq("csr"), eq(false));
  }

  @Test
  void renewalRequestIsStillRejected() {
    SCMSecurityRequest request = wrap(scmCertRequestBuilder().setRenew(true));

    ServiceException ex = assertThrows(ServiceException.class,
        () -> translator.submitRequest(null, request));
    assertInstanceOf(ServerNotLeaderException.class, ex.getCause());
  }

  @Test
  void nonScmCertificateRequestIsStillRejectedInLeaderlessPrimaryState() {
    SCMGetOMCertRequestProto omRequest = SCMGetOMCertRequestProto.newBuilder()
        .setOmDetails(OzoneManagerDetailsProto.newBuilder()
            .setUuid("om-1").setIpAddress("10.0.0.2").setHostName("om1.example.com").build())
        .setCSR("csr")
        .build();
    SCMSecurityRequest getOMCertificate = SCMSecurityRequest.newBuilder()
        .setCmdType(Type.GetOMCertificate)
        .setTraceID("trace-2")
        .setGetOMCertRequest(omRequest)
        .build();
    assertThrows(ServiceException.class, () -> translator.submitRequest(null, getOMCertificate));
  }
}
