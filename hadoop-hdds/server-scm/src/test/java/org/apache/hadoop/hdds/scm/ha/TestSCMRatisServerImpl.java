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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

/**
 * Test for SCM Ratis Server Implementation.
 */
public class TestSCMRatisServerImpl {

  @TempDir
  private Path tempDir;

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
  public void testInitializeSkipEntriesOfStaleGroup() throws Exception {
    final int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR, tempDir.resolve("ratis").toString());
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, port);
    final SCMNodeDetails details = new SCMNodeDetails.Builder()
        .setRpcAddress(new InetSocketAddress("localhost", 0))
        .setRatisPort(port)
        .setSCMNodeId("scm1")
        .build();

    // A previous SCM initialized its group and committed a data entry.
    final String oldClusterId = "CID-" + UUID.randomUUID();
    final String oldScmId = UUID.randomUUID().toString();
    SCMRatisServerImpl.initialize(oldClusterId, oldScmId, details, conf);
    final RaftGroup oldGroup = RaftGroup.valueOf(SCMRatisServerImpl.buildRaftGroupId(oldClusterId),
        RaftPeer.newBuilder().setId(RaftPeerId.getRaftPeerId(oldScmId)).setAddress("localhost:" + port).build());
    final RaftProperties properties = RatisUtil.newRaftProperties(conf);
    try (RaftServer server = RaftServer.newBuilder().setServerId(RaftPeerId.getRaftPeerId(oldScmId))
        .setProperties(properties).setGroup(oldGroup).setStateMachine(new BaseStateMachine())
        .setOption(RaftStorage.StartupOption.RECOVER).build();
         RaftClient client = RaftClient.newBuilder().setProperties(properties).setRaftGroup(oldGroup)
             .setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties))
             .build()) {
      server.start();
      assertTrue(client.io().send(SCMRatisRequest.of(RequestType.CONTAINER, "op", new Class<?>[0]).encode())
          .isSuccess());
    }

    // The Ratis directory is kept while the SCM is initialized again with a new cluster ID and SCM ID.
    final LogCapturer logs = LogCapturer.captureLogs(
        LoggerFactory.getLogger("org.apache.ratis.server.impl.StateMachineUpdater"));
    final LogCapturer smLogs = LogCapturer.captureLogs(SCMStateMachine.class);
    try {
      SCMRatisServerImpl.initialize("CID-" + UUID.randomUUID(), UUID.randomUUID().toString(), details, conf);

      assertThat(smLogs.getOutput()).contains("Skipping applyTransaction");
      assertThat(logs.getOutput()).doesNotContain("NullPointerException");
    } finally {
      logs.stopCapturing();
      smLogs.stopCapturing();
    }
  }
}
