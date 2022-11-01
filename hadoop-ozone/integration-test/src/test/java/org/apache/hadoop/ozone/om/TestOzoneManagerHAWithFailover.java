/**
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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.NODE_FAILURE_TIMEOUT;

/**
 * Test Ozone Manager operation in distributed handler scenario with failover.
 * NOTE: Do not add new tests to this class since
 * testIncrementalWaitTimeWithSameNodeFailove does not leave the cluster in a
 * reusable state.
 */
public class TestOzoneManagerHAWithFailover extends TestOzoneManagerHA {
  /**
   * 1. Stop one of the OM
   * 2. make a call to OM, this will make failover attempts to find new node.
   * a) if LE finishes but leader not ready, it retries to same node
   * b) if LE not done, it will failover to new node and check
   * 3. Try failover to same OM explicitly.
   * Now #3 should wait additional waitBetweenRetries time.
   * LE: Leader Election.
   */
  @Test
  public void testIncrementalWaitTimeWithSameNodeFailover() throws Exception {
    long waitBetweenRetries = getConf().getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFailoverProxyProvider(getObjectStore().getClientProxy());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    getCluster().stopOzoneManager(leaderOMNodeId);
    Thread.sleep(NODE_FAILURE_TIMEOUT * 4);
    createKeyTest(true); // failover should happen to new node

    long numTimesTriedToSameNode = omFailoverProxyProvider.getWaitTime()
        / waitBetweenRetries;
    omFailoverProxyProvider.setNextOmProxy(omFailoverProxyProvider.
        getCurrentProxyOMNodeId());
    Assert.assertEquals((numTimesTriedToSameNode + 1) * waitBetweenRetries,
        omFailoverProxyProvider.getWaitTime());
  }

  /**
   * Choose a follower to send the request, the returned exception should
   * include the suggested leader node.
   */
  @Test
  public void testFailoverWithSuggestedLeader() {
    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil
            .getFailoverProxyProvider(getObjectStore().getClientProxy());

    // The OMFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();
    String leaderOMAddress = ((OMProxyInfo)
        omFailoverProxyProvider.getOMProxyInfoMap().get(leaderOMNodeId))
        .getAddress().getAddress().toString();
    OzoneManager followerOM = null;
    for (OzoneManager om: getCluster().getOzoneManagersList()) {
      if (!om.isLeaderReady()) {
        followerOM = om;
        break;
      }
    }
    Assertions.assertTrue(followerOM.getOmRatisServer().checkLeaderStatus() ==
        OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER);

    OzoneManagerProtocolProtos.OMRequest writeRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.ListVolume)
            .setVersion(ClientVersion.CURRENT_VERSION)
            .setClientId(UUID.randomUUID().toString())
            .build();

    try {
      OzoneManagerProtocolProtos.OMResponse
          omResponse = followerOM.getOmServerProtocol()
          .submitRequest(null, writeRequest);
      Assertions.fail("Test failure with NotLeaderException");
    } catch (Exception ex) {
      GenericTestUtils.assertExceptionContains("Suggested leader is OM:" +
          leaderOMNodeId + leaderOMAddress, ex);
    }
  }
}
