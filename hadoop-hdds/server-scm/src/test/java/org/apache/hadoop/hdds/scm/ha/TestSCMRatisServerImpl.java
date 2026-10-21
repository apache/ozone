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

package org.apache.hadoop.hdds.scm.ha;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.UUID;
import javax.net.ssl.X509TrustManager;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509KeyManager;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509TrustManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

/**
 * Test for SCM Ratis Server Implementation.
 */
public class TestSCMRatisServerImpl {

  @Test
  public void  testGetLeaderId() throws Exception {

    try (
        MockedConstruction<SecurityConfig> mockedSecurityConfigConstruction = mockConstruction(SecurityConfig.class);
        MockedStatic<RaftServer> staticMockedRaftServer = mockStatic(RaftServer.class);
        MockedStatic<RatisUtil> staticMockedRatisUtil = mockStatic(RatisUtil.class);
    ) {
      // given
      ConfigurationSource conf = mock(ConfigurationSource.class);
      StorageContainerManager scm = mock(StorageContainerManager.class);
      String clusterId = "CID-" + UUID.randomUUID();
      when(scm.getClusterId()).thenReturn(clusterId);
      SCMHADBTransactionBuffer dbTransactionBuffer = mock(SCMHADBTransactionBuffer.class);

      RaftServer.Builder raftServerBuilder = mock(RaftServer.Builder.class);
      when(raftServerBuilder.setServerId(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setProperties(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setStateMachineRegistry(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setOption(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setGroup(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setParameters(any())).thenReturn(raftServerBuilder);

      RaftServer raftServer = mock(RaftServer.class);

      RaftServer.Division division = mock(RaftServer.Division.class);
      when(raftServer.getDivision(any())).thenReturn(division);

      SCMStateMachine scmStateMachine = mock(SCMStateMachine.class);
      when(division.getStateMachine()).thenReturn(scmStateMachine);

      when(raftServerBuilder.build()).thenReturn(raftServer);

      staticMockedRaftServer.when(RaftServer::newBuilder).thenReturn(raftServerBuilder);

      RaftProperties raftProperties = mock(RaftProperties.class);
      staticMockedRatisUtil.when(() -> RatisUtil.newRaftProperties(conf)).thenReturn(raftProperties);

      SecurityConfig sc = new SecurityConfig(conf);
      when(sc.isSecurityEnabled()).thenReturn(false);

      SCMRatisServerImpl scmRatisServer = spy(new SCMRatisServerImpl(conf, scm, dbTransactionBuffer));
      doReturn(RaftPeer.newBuilder().setId(RaftPeerId.valueOf("peer1")).build()).when(scmRatisServer).getLeader();

      // when
      RaftPeerId leaderId = scmRatisServer.getLeaderId();

      // then
      assertEquals(RaftPeerId.valueOf("peer1"), leaderId);

      // but when
      doReturn(null).when(scmRatisServer).getLeader();
      leaderId = scmRatisServer.getLeaderId();

      // then
      assertNull(leaderId);
    }
  }

  @Test
  public void testTlsConfigAcceptsOnlyScmCertificates() throws Exception {
    SecurityConfig securityConfig = mock(SecurityConfig.class);
    when(securityConfig.isSecurityEnabled()).thenReturn(true);
    when(securityConfig.isGrpcTlsEnabled()).thenReturn(true);
    ReloadingX509TrustManager clusterTrustManager = mock(ReloadingX509TrustManager.class);
    CertificateClient certificateClient = mock(CertificateClient.class);
    when(certificateClient.getKeyManager()).thenReturn(mock(ReloadingX509KeyManager.class));
    when(certificateClient.getTrustManager()).thenReturn(clusterTrustManager);

    X509TrustManager trustManager = (X509TrustManager) HASecurityUtils
        .createSCMRatisTLSConfig(securityConfig, certificateClient).getTrustManager().getTrustManager();

    X509Certificate scmCert = mock(X509Certificate.class);
    when(scmCert.getBasicConstraints()).thenReturn(Integer.MAX_VALUE);
    X509Certificate[] scmChain = {scmCert};
    trustManager.checkClientTrusted(scmChain, "RSA");
    trustManager.checkServerTrusted(scmChain, "RSA");

    X509Certificate leafCert = mock(X509Certificate.class);
    when(leafCert.getBasicConstraints()).thenReturn(-1);
    X509Certificate[] leafChain = {leafCert};
    assertThatThrownBy(() -> trustManager.checkClientTrusted(leafChain, "RSA"))
        .isInstanceOf(CertificateException.class)
        .hasMessageStartingWith("Not an SCM certificate");
    assertThatThrownBy(() -> trustManager.checkServerTrusted(leafChain, "RSA"))
        .isInstanceOf(CertificateException.class)
        .hasMessageStartingWith("Not an SCM certificate");
    // Only the peer certificate counts, not the CA certificates that follow it in the chain.
    X509Certificate[] leafWithIssuerChain = {leafCert, scmCert};
    assertThatThrownBy(() -> trustManager.checkClientTrusted(leafWithIssuerChain, "RSA"))
        .isInstanceOf(CertificateException.class)
        .hasMessageStartingWith("Not an SCM certificate");

    // A chain the cluster trust manager rejects stays rejected.
    doThrow(new CertificateException("untrusted chain")).when(clusterTrustManager).checkClientTrusted(scmChain, "RSA");
    assertThatThrownBy(() -> trustManager.checkClientTrusted(scmChain, "RSA")).hasMessage("untrusted chain");
  }
}
