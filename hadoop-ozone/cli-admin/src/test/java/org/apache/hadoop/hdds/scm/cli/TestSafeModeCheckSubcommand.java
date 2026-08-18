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

package org.apache.hadoop.hdds.scm.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests leader detection in `ozone admin safemode status`, which picks its
 * target node by parsing the Ratis role strings returned by SCM.
 */
public class TestSafeModeCheckSubcommand {

  private static final String LEADER_ID = "e428ca07-b2a3-4756-bf9b-a4abb033c7d1";
  private static final String FOLLOWER_ID = "61b1c8e5-da40-4567-8a17-96a0234ba14e";

  private static SCMNodeInfo node(String nodeId, String host) {
    HostAndPort address = new HostAndPort(host, 9860);
    return new SCMNodeInfo("scmservice", nodeId, address, address, address, address);
  }

  private static SCMNodeInfo findLeader(List<String> roles, List<SCMNodeInfo> nodes)
      throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.getScmRoles()).thenReturn(roles);

    return SafeModeCheckSubcommand.findLeaderNode(scmClient, nodes);
  }

  @Test
  public void testFindLeaderNodeIPv4() throws Exception {
    SCMNodeInfo scm1 = node("scm1", "10.0.0.1");
    SCMNodeInfo scm2 = node("scm2", "10.0.0.2");

    SCMNodeInfo leader = findLeader(
        Arrays.asList(
            "scm2.example.com:9894:FOLLOWER:" + FOLLOWER_ID + ":10.0.0.2",
            "scm1.example.com:9894:LEADER:" + LEADER_ID + ":10.0.0.1"),
        Arrays.asList(scm1, scm2));

    // The configured nodes carry addresses, not DNS names, so the match comes
    // from the trailing resolved-address field rather than the host field.
    assertEquals(scm1, leader);
  }

  @Test
  public void testFindLeaderNodeIPv6() throws Exception {
    SCMNodeInfo scm1 = node("scm1", "2001:db8::1");
    SCMNodeInfo scm2 = node("scm2", "2001:db8::2");

    SCMNodeInfo leader = findLeader(
        Arrays.asList(
            "[2001:db8::2]:9894:FOLLOWER:" + FOLLOWER_ID + ":[2001:db8:0:0:0:0:0:2]",
            "[2001:db8::1]:9894:LEADER:" + LEADER_ID + ":[2001:db8:0:0:0:0:0:1]"),
        Arrays.asList(scm1, scm2));

    assertEquals(scm1, leader);
  }

  @Test
  public void testFindLeaderNodeMatchesEquivalentIPv6Forms() throws Exception {
    // SCM reports the compressed form while the node is configured with the
    // expanded one; the two denote the same address and must still match.
    SCMNodeInfo scm1 = node("scm1", "2001:db8:0:0:0:0:0:1");

    SCMNodeInfo leader = findLeader(
        Arrays.asList("[2001:db8::1]:9894:LEADER:" + LEADER_ID + ":[2001:db8::1]"),
        Arrays.asList(scm1));

    assertEquals(scm1, leader);
  }

  @Test
  public void testFindLeaderNodeWithoutLeader() throws Exception {
    SCMNodeInfo leader = findLeader(
        Arrays.asList("[2001:db8::1]:9894:FOLLOWER:" + FOLLOWER_ID + ":[2001:db8::1]"),
        Arrays.asList(node("scm1", "2001:db8::1")));

    assertNull(leader);
  }

  /**
   * An empty entry is what the producer emits for a peer with no address, so
   * it reaches leader detection like any other role string.
   */
  @ParameterizedTest
  @ValueSource(strings = {"", "Exception Occurred, No leader found"})
  public void testFindLeaderNodeSkipsUnparseableRole(String unparseable) throws Exception {
    SCMNodeInfo scm1 = node("scm1", "2001:db8::1");

    SCMNodeInfo leader = findLeader(
        Arrays.asList(unparseable, "[2001:db8::1]:9894:LEADER:" + LEADER_ID + ":[2001:db8::1]"),
        Arrays.asList(scm1));

    assertEquals(scm1, leader);
  }
}
