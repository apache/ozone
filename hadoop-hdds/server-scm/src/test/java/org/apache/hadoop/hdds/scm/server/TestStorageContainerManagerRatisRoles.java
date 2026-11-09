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

package org.apache.hadoop.hdds.scm.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the SCM roles exposed over JMX, which the SCM web UI renders. The
 * columns are reordered relative to the encoded role string, so the mapping
 * is asserted field by field.
 */
public class TestStorageContainerManagerRatisRoles {

  private static final String LEADER_ID = "e428ca07-b2a3-4756-bf9b-a4abb033c7d1";
  private static final String FOLLOWER_ID = "61b1c8e5-da40-4567-8a17-96a0234ba14e";

  private static StorageContainerManager scmWith(SCMRatisServer server) {
    SCMHAManager haManager = mock(SCMHAManager.class);
    when(haManager.getRatisServer()).thenReturn(server);

    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getScmHAManager()).thenReturn(haManager);
    doCallRealMethod().when(scm).getScmRatisRoles();
    return scm;
  }

  private static SCMRatisServer ratisServer(RaftPeerId leaderId, List<String> roles) {
    DivisionInfo info = mock(DivisionInfo.class);
    when(info.getLeaderId()).thenReturn(leaderId);

    RaftServer.Division division = mock(RaftServer.Division.class);
    when(division.getInfo()).thenReturn(info);

    SCMRatisServer server = mock(SCMRatisServer.class);
    when(server.getDivision()).thenReturn(division);
    when(server.isStopped()).thenReturn(false);
    when(server.getRatisRoles()).thenReturn(roles);
    return server;
  }

  private static SCMRatisServer healthyServer(List<String> roles) {
    return ratisServer(RaftPeerId.valueOf("scm1"), roles);
  }

  @Test
  public void testGetScmRatisRolesIPv4() {
    StorageContainerManager scm = scmWith(healthyServer(Arrays.asList(
        "scm1.example.com:9894:LEADER:" + LEADER_ID + ":10.0.0.1",
        "scm2.example.com:9894:FOLLOWER:" + FOLLOWER_ID + ":10.0.0.2")));

    assertEquals(
        Arrays.asList(
            Arrays.asList("scm1.example.com", LEADER_ID, "9894", "LEADER"),
            Arrays.asList("scm2.example.com", FOLLOWER_ID, "9894", "FOLLOWER")),
        scm.getScmRatisRoles());
  }

  @Test
  public void testGetScmRatisRolesIPv6() {
    StorageContainerManager scm = scmWith(healthyServer(Arrays.asList(
        "[2001:db8::1]:9894:LEADER:" + LEADER_ID + ":[2001:db8:0:0:0:0:0:1]",
        "[2001:db8::2]:9894:FOLLOWER:" + FOLLOWER_ID + ":[2001:db8:0:0:0:0:0:2]")));

    // The host column is a display value, so the brackets that make the
    // encoded field unambiguous are stripped back off.
    assertEquals(
        Arrays.asList(
            Arrays.asList("2001:db8::1", LEADER_ID, "9894", "LEADER"),
            Arrays.asList("2001:db8::2", FOLLOWER_ID, "9894", "FOLLOWER")),
        scm.getScmRatisRoles());
  }

  @Test
  public void testGetScmRatisRolesWithoutLeader() {
    StorageContainerManager scm = scmWith(ratisServer(null, Collections.emptyList()));

    assertEquals(Collections.singletonList(Collections.singletonList("No leader found")),
        scm.getScmRatisRoles());
  }

  /**
   * Verify that the JMX view reports invalid role strings as an error row.
   */
  @ParameterizedTest
  @ValueSource(strings = {"", "scm1.example.com:9894"})
  public void testGetScmRatisRolesWithUnparseableRole(String role) {
    StorageContainerManager scm = scmWith(healthyServer(Collections.singletonList(role)));

    List<List<String>> roles = scm.getScmRatisRoles();

    assertEquals(1, roles.size());
    assertEquals(1, roles.get(0).size());
    assertThat(roles.get(0).get(0)).startsWith("Exception Occurred, ");
  }
}
