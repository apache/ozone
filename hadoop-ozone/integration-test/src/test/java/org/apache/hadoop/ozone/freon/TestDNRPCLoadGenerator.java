/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests Freon, with MiniOzoneCluster and validate data.
 */
public class TestDNRPCLoadGenerator {

  private static MiniOzoneCluster cluster = null;
  private static ContainerWithPipeline container;

  private static void startCluster(OzoneConfiguration conf) throws Exception {
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
            180000);

    StorageContainerLocationProtocolClientSideTranslatorPB
        storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    container =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    XceiverClientManager xceiverClientManager = new XceiverClientManager(conf);
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client,
        container.getContainerInfo().getContainerID(), null);
  }

  static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    startCluster(conf);
  }

  @AfterAll
  public static void shutdown() {
    shutdownCluster();
  }

  @Test
  public void test() {
    DNRPCLoadGenerator randomKeyGenerator =
        new DNRPCLoadGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    int exitCode = cmd.execute(
        "--container-id", Long.toString(container.getContainerInfo().getContainerID()),
        "--clients", "5",
        "-t", "10");
    assertEquals(0, exitCode);
  }
}
