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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for Ratis tests.
 */
public interface RatisTestHelper {
  Logger LOG = LoggerFactory.getLogger(RatisTestHelper.class);

  static void initRatisConf(RpcType rpc, OzoneConfiguration conf) {
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY, rpc.name());
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 30, TimeUnit.SECONDS);
    LOG.info("{} = {}", OzoneConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY,
            rpc.name());
  }

  static void initXceiverServerRatis(
      RpcType rpc, DatanodeDetails dd, Pipeline pipeline) throws IOException {
    final RaftPeer p = RatisHelper.toRaftPeer(dd);
    final OzoneConfiguration conf = new OzoneConfiguration();
    try (RaftClient client = RatisHelper.newRaftClient(
        rpc, p, RatisHelper.createRetryPolicy(conf), conf)) {
      client.getGroupManagementApi(p.getId())
          .add(RatisHelper.newRaftGroup(pipeline));
    }
  }

  static RaftServer.Division getRaftServerDivision(
      HddsDatanodeService dn, Pipeline pipeline) throws Exception {
    if (!pipeline.getNodes().contains(dn.getDatanodeDetails())) {
      throw new IllegalArgumentException("Pipeline:" + pipeline.getId() +
          " not exist in datanode:" + dn.getDatanodeDetails().getUuid());
    }

    XceiverServerRatis server =
        (XceiverServerRatis) (dn.getDatanodeStateMachine().
        getContainer().getWriteChannel());
    return server.getServerDivision(
        RatisHelper.newRaftGroup(pipeline).getGroupId());
  }

  static StateMachine getStateMachine(HddsDatanodeService dn,
      Pipeline pipeline) throws Exception {
    return getRaftServerDivision(dn, pipeline).getStateMachine();
  }

  static boolean isRatisLeader(HddsDatanodeService dn, Pipeline pipeline)
      throws Exception {
    return getRaftServerDivision(dn, pipeline).getInfo().isLeader();
  }

  static boolean isRatisFollower(HddsDatanodeService dn,
      Pipeline pipeline) throws Exception {
    return getRaftServerDivision(dn, pipeline).getInfo().isFollower();
  }
}
