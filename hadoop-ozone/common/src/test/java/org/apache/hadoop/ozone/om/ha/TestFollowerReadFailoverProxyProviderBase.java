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

package org.apache.hadoop.ozone.om.ha;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link FollowerReadFailoverProxyProviderBase}.
 */
public class TestFollowerReadFailoverProxyProviderBase {
  private static final String OM_SERVICE_ID = "om-service-test1";

  private HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy;
  /** The OM nodes in the order used for follower read. */
  private List<String> nodeIds;

  @BeforeEach
  void setUp() throws IOException {
    leaderProxy = newLeaderProxy("om1", "om2", "om3");
    nodeIds = new ArrayList<>(leaderProxy.getOMProxyMap().getNodeIds());
  }

  @Test
  void testRejectInvalidReadConsistency() {
    assertThrows(IllegalStateException.class, () -> new FollowerReadProvider(leaderProxy,
        ReadConsistency.LINEARIZABLE_LEADER_ONLY, ReadConsistency.DEFAULT, true));
    assertThrows(IllegalStateException.class, () -> new FollowerReadProvider(leaderProxy,
        ReadConsistency.LOCAL_LEASE, ReadConsistency.LOCAL_LEASE, true));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testReadWithoutHintUsesFollowerReadDefault(boolean defaultFollowerReadEnabled) {
    final FollowerReadProvider provider = newProvider(defaultFollowerReadEnabled);

    assertEquals(defaultFollowerReadEnabled, provider.shouldUseFollowerRead(newRequest(Type.ListVolume)));
    assertFalse(provider.shouldUseFollowerRead(newRequest(Type.CreateVolume)));
  }

  @Test
  void testReadConsistencyHintOverridesFollowerReadDefault() {
    final FollowerReadProvider disabledByDefault = newProvider(false);
    assertTrue(disabledByDefault.shouldUseFollowerRead(newReadRequest(ReadConsistency.LOCAL_LEASE)));
    assertTrue(disabledByDefault.shouldUseFollowerRead(newReadRequest(ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER)));

    final FollowerReadProvider enabledByDefault = newProvider(true);
    assertFalse(enabledByDefault.shouldUseFollowerRead(newReadRequest(ReadConsistency.LINEARIZABLE_LEADER_ONLY)));
    assertFalse(enabledByDefault.shouldUseFollowerRead(newReadRequest(ReadConsistency.DEFAULT)));
  }

  @Test
  void testNoFollowerReadAfterFollowerReadIsDisabled() {
    final FollowerReadProvider provider = newProvider(true);
    assertTrue(provider.isOmServiceSupportsFollowerRead());

    provider.disableFollowerRead();

    assertFalse(provider.isOmServiceSupportsFollowerRead());
    assertFalse(provider.shouldUseFollowerRead(newRequest(Type.ListVolume)));
    assertFalse(provider.shouldUseFollowerRead(newReadRequest(ReadConsistency.LOCAL_LEASE)));
  }

  @Test
  void testAddReadConsistencyHint() {
    final FollowerReadProvider provider = newProvider(true);

    final OMRequest withoutHint = newRequest(Type.ListVolume);
    assertEquals(ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER.getHint(),
        provider.addReadConsistencyHint(withoutHint, true).getReadConsistencyHint());
    assertEquals(ReadConsistency.DEFAULT.getHint(),
        provider.addReadConsistencyHint(withoutHint, false).getReadConsistencyHint());

    // An existing hint is kept.
    final OMRequest withHint = newReadRequest(ReadConsistency.LOCAL_LEASE);
    assertSame(withHint, provider.addReadConsistencyHint(withHint, true));
    assertSame(withHint, provider.addReadConsistencyHint(withHint, false));
  }

  @Test
  void testChangeFollowerReadNode() {
    final FollowerReadProvider provider = newProvider(true);
    assertEquals(nodeIds.get(0), provider.getCurrentFollowerReadNodeId());

    provider.changeFollowerReadNodeId(nodeIds.get(0));
    assertEquals(nodeIds.get(1), provider.getCurrentFollowerReadNodeId());

    // A change from a node which is no longer the current node is ignored.
    provider.changeFollowerReadNodeId(nodeIds.get(0));
    assertEquals(nodeIds.get(1), provider.getCurrentFollowerReadNodeId());

    // The change goes back to the first node after the last node.
    provider.changeFollowerReadNodeId(nodeIds.get(1));
    provider.changeFollowerReadNodeId(nodeIds.get(2));
    assertEquals(nodeIds.get(0), provider.getCurrentFollowerReadNodeId());

    provider.changeInitialProxyForTest(nodeIds.get(2));
    assertEquals(nodeIds.get(2), provider.getCurrentFollowerReadNodeId());
  }

  @Test
  void testOnlyLocalLeaseReadSkipsLeader() {
    final FollowerReadProvider provider = newProvider(true);
    changeLeader(nodeIds.get(0));

    assertEquals(nodeIds.get(0), provider.selectFollowerReadNodeId(ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER));
    assertEquals(nodeIds.get(1), provider.selectFollowerReadNodeId(ReadConsistency.LOCAL_LEASE));
    assertEquals(nodeIds.get(1), provider.getCurrentFollowerReadNodeId());
  }

  @Test
  void testLocalLeaseReadWithoutFollower() throws Exception {
    final FollowerReadProvider provider = new FollowerReadProvider(newLeaderProxy("om1"),
        ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER, ReadConsistency.DEFAULT, true);

    assertNull(provider.selectFollowerReadNodeId(ReadConsistency.LOCAL_LEASE));
  }

  private static HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> newLeaderProxy(String... omNodeIds)
      throws IOException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID), String.join(",", omNodeIds));
    for (String nodeId : omNodeIds) {
      conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID, nodeId), "0.0.0.0:8080");
    }
    return new HadoopRpcOMFailoverProxyProvider<>(conf, UserGroupInformation.getCurrentUser(),
        OM_SERVICE_ID, OzoneManagerProtocolPB.class);
  }

  private FollowerReadProvider newProvider(boolean defaultFollowerReadEnabled) {
    return new FollowerReadProvider(leaderProxy, ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER,
        ReadConsistency.DEFAULT, defaultFollowerReadEnabled);
  }

  private void changeLeader(String nodeId) {
    leaderProxy.setNextOmProxy(nodeId);
    leaderProxy.performFailover(null);
  }

  private static OMRequest newRequest(Type cmdType) {
    return OMRequest.newBuilder()
        .setCmdType(cmdType)
        .setClientId("test")
        .build();
  }

  private static OMRequest newReadRequest(ReadConsistency readConsistency) {
    return newRequest(Type.ListVolume).toBuilder()
        .setReadConsistencyHint(readConsistency.getHint())
        .build();
  }

  /** A provider with only the base class logic. */
  private static final class FollowerReadProvider extends FollowerReadFailoverProxyProviderBase {
    private final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy;

    FollowerReadProvider(HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> leaderProxy,
        ReadConsistency followerReadConsistency, ReadConsistency leaderReadConsistency,
        boolean defaultFollowerReadEnabled) {
      super(followerReadConsistency, leaderReadConsistency, defaultFollowerReadEnabled);
      this.leaderProxy = leaderProxy;
    }

    @Override
    protected HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> getLeaderProxy() {
      return leaderProxy;
    }
  }
}
