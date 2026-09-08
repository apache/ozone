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

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
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
  public void testGetRatisRolesWithIPv6() throws Exception {
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

      // IPv6 peer address in the bracketed format that getRatisHostPortStr() produces
      RaftPeer ipv6Peer = RaftPeer.newBuilder()
          .setId(RaftPeerId.valueOf("peer1"))
          .setAddress("[2001:db8::1]:9894")
          .build();

      RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.randomId(), ipv6Peer);
      when(division.getGroup()).thenReturn(raftGroup);
      doReturn(ipv6Peer).when(scmRatisServer).getLeader();

      List<String> roles = scmRatisServer.getRatisRoles();
      assertEquals(1, roles.size());

      String roleString = roles.get(0);
      String[] parsed = HddsUtils.parseRatisRoleString(roleString);
      assertEquals("2001:db8::1", parsed[0]);
      assertEquals("9894", parsed[1]);
      assertEquals("LEADER", parsed[2]);
      assertEquals("peer1", parsed[3]);
      assertTrue(!parsed[4].isEmpty(), "hostIP should be resolved");
    }
  }

}
