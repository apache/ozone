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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.net.ssl.X509TrustManager;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509KeyManager;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509TrustManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
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

  static Stream<Arguments> peerAddressEncodings() {
    return Stream.of(
        Arguments.of("10.0.0.1:9894", "10.0.0.1:9894:LEADER:peer1:10.0.0.1"),
        Arguments.of("[2001:db8::1]:9894", "[2001:db8::1]:9894:LEADER:peer1:[2001:db8::1]"));
  }

  /**
   * The encoding is a wire format shared with every consumer of
   * {@code ozone admin scm roles}, so it is asserted verbatim rather than
   * round-tripped through the parser that reads it back.
   */
  @ParameterizedTest
  @MethodSource("peerAddressEncodings")
  public void testGetRatisRolesEncoding(String peerAddress, String expectedRole) throws Exception {
    try (
        MockedConstruction<SecurityConfig> mockedSecurityConfigConstruction = mockConstruction(SecurityConfig.class);
        MockedStatic<RaftServer> staticMockedRaftServer = mockStatic(RaftServer.class);
        MockedStatic<RatisUtil> staticMockedRatisUtil = mockStatic(RatisUtil.class);
    ) {
      ConfigurationSource conf = mock(ConfigurationSource.class);
      StorageContainerManager scm = mock(StorageContainerManager.class);
      when(scm.getClusterId()).thenReturn("CID-" + UUID.randomUUID());
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
      when(raftServerBuilder.build()).thenReturn(raftServer);
      staticMockedRaftServer.when(RaftServer::newBuilder).thenReturn(raftServerBuilder);

      RaftProperties raftProperties = mock(RaftProperties.class);
      staticMockedRatisUtil.when(() -> RatisUtil.newRaftProperties(conf)).thenReturn(raftProperties);

      SecurityConfig sc = new SecurityConfig(conf);
      when(sc.isSecurityEnabled()).thenReturn(false);

      SCMRatisServerImpl scmRatisServer = spy(new SCMRatisServerImpl(conf, scm, dbTransactionBuffer));

      RaftPeer peer = RaftPeer.newBuilder()
          .setId(RaftPeerId.valueOf("peer1"))
          .setAddress(peerAddress)
          .build();

      when(division.getGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId(), peer));
      doReturn(peer).when(scmRatisServer).getLeader();

      List<String> roles = scmRatisServer.getRatisRoles();

      assertEquals(Arrays.asList(expectedRole), roles);
    }
  }

  @Test
  public void testGetRatisRolesKeepsHostname() throws Exception {
    try (
        MockedConstruction<SecurityConfig> mockedSecurityConfigConstruction = mockConstruction(SecurityConfig.class);
        MockedStatic<RaftServer> staticMockedRaftServer = mockStatic(RaftServer.class);
        MockedStatic<RatisUtil> staticMockedRatisUtil = mockStatic(RatisUtil.class);
    ) {
      ConfigurationSource conf = mock(ConfigurationSource.class);
      StorageContainerManager scm = mock(StorageContainerManager.class);
      when(scm.getClusterId()).thenReturn("CID-" + UUID.randomUUID());
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
      when(raftServerBuilder.build()).thenReturn(raftServer);
      staticMockedRaftServer.when(RaftServer::newBuilder).thenReturn(raftServerBuilder);

      RaftProperties raftProperties = mock(RaftProperties.class);
      staticMockedRatisUtil.when(() -> RatisUtil.newRaftProperties(conf)).thenReturn(raftProperties);

      SecurityConfig sc = new SecurityConfig(conf);
      when(sc.isSecurityEnabled()).thenReturn(false);

      SCMRatisServerImpl scmRatisServer = spy(new SCMRatisServerImpl(conf, scm, dbTransactionBuffer));

      RaftPeer peer = RaftPeer.newBuilder()
          .setId(RaftPeerId.valueOf("peer1"))
          .setAddress("localhost:9894")
          .build();

      when(division.getGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId(), peer));
      doReturn(peer).when(scmRatisServer).getLeader();

      List<String> roles = scmRatisServer.getRatisRoles();

      // Which address a name resolves to is the resolver's business, so only
      // the fields the encoding itself owns are pinned here.
      assertEquals(1, roles.size());
      assertThat(roles.get(0)).startsWith("localhost:9894:LEADER:peer1:");
      assertThat(roles.get(0)).doesNotEndWith(":");
    }
  }

  @Test
  public void testSubmitRequestIsTracedUnderTheCurrentSpan() throws Exception {
    ArgumentCaptor<RaftClientRequest> submitted = ArgumentCaptor.forClass(RaftClientRequest.class);
    SCMRatisServerImpl scmRatisServer = newScmRatisServer(submitted, null);
    SCMRatisRequest request = SCMRatisRequest.of(RequestType.PIPELINE, "op", new Class<?>[0]);

    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      TracingUtil.executeInNewSpan("request", () -> scmRatisServer.submitRequest(request));

      collector.assertParent("SCMRatisServerImpl.submit.op", "request");
      // The request is encoded inside the span, so the apply span on each SCM continues from it.
      String traceID = SCMRatisRequest.decode(submitted.getValue().getMessage()).getTraceID();
      TracingUtil.executeAsChildSpan("apply", traceID, () -> { });
      collector.assertParent("apply", "SCMRatisServerImpl.submit.op");
    }
  }

  @Test
  public void testRejectedSubmitRequestIsRecordedOnSpan() throws Exception {
    SCMRatisServerImpl scmRatisServer = newScmRatisServer(ArgumentCaptor.forClass(RaftClientRequest.class),
        new StateMachineException("rejected"));
    SCMRatisRequest request = SCMRatisRequest.of(RequestType.PIPELINE, "op", new Class<?>[0]);

    try (TestSpanCollector collector = TestSpanCollector.install("scm")) {
      assertFalse(scmRatisServer.submitRequest(request).isSuccess());

      collector.assertError("SCMRatisServerImpl.submit.op", "rejected");
      collector.assertExceptionRecorded("SCMRatisServerImpl.submit.op", StateMachineException.class);
    }
  }

  /**
   * Returns a server whose Ratis server records each request in {@code submitted} and replies with {@code exception},
   * or with success if it is null.
   */
  private static SCMRatisServerImpl newScmRatisServer(ArgumentCaptor<RaftClientRequest> submitted,
      RaftException exception) throws Exception {
    try (
        MockedConstruction<SecurityConfig> mockedSecurityConfigConstruction = mockConstruction(SecurityConfig.class);
        MockedStatic<RaftServer> staticMockedRaftServer = mockStatic(RaftServer.class);
        MockedStatic<RatisUtil> staticMockedRatisUtil = mockStatic(RatisUtil.class);
    ) {
      ConfigurationSource conf = mock(ConfigurationSource.class);
      StorageContainerManager scm = mock(StorageContainerManager.class);
      when(scm.getClusterId()).thenReturn("CID-" + UUID.randomUUID());

      RaftServer.Builder raftServerBuilder = mock(RaftServer.Builder.class);
      when(raftServerBuilder.setServerId(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setProperties(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setStateMachineRegistry(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setOption(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setGroup(any())).thenReturn(raftServerBuilder);
      when(raftServerBuilder.setParameters(any())).thenReturn(raftServerBuilder);

      RaftPeer peer = RaftPeer.newBuilder().setId(RaftPeerId.valueOf("peer1")).build();
      RaftServer.Division division = mock(RaftServer.Division.class);
      when(division.getId()).thenReturn(peer.getId());
      when(division.getGroup()).thenReturn(RaftGroup.valueOf(RaftGroupId.randomId(), peer));

      RaftClientReply reply = mock(RaftClientReply.class);
      when(reply.isSuccess()).thenReturn(exception == null);
      when(reply.getException()).thenReturn(exception);
      when(reply.getMessage()).thenReturn(Message.EMPTY);
      RaftServer raftServer = mock(RaftServer.class);
      when(raftServer.getDivision(any())).thenReturn(division);
      when(raftServer.submitClientRequestAsync(submitted.capture()))
          .thenReturn(CompletableFuture.completedFuture(reply));
      when(raftServerBuilder.build()).thenReturn(raftServer);
      staticMockedRaftServer.when(RaftServer::newBuilder).thenReturn(raftServerBuilder);

      staticMockedRatisUtil.when(() -> RatisUtil.newRaftProperties(conf)).thenReturn(mock(RaftProperties.class));

      return new SCMRatisServerImpl(conf, scm, mock(SCMHADBTransactionBuffer.class));
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
