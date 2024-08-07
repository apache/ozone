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
import org.apache.hadoop.hdds.scm.XceiverClientCreator;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

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
    try (XceiverClientFactory factory = new XceiverClientCreator(conf);
        XceiverClientSpi client = factory.acquireClient(container.getPipeline())) {
      ContainerProtocolCalls.createContainer(client,
          container.getContainerInfo().getContainerID(), null);
    }
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

  private static Stream<Arguments> provideParameters() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false)
    );
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void test(boolean readOnly, boolean ratis) {
    DNRPCLoadGenerator randomKeyGenerator =
        new DNRPCLoadGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    List<String> cmdArgs = new ArrayList<>(Arrays.asList(
        "--container-id", Long.toString(container.getContainerInfo().getContainerID()),
        "--clients", "5",
        "-t", "10"));

    if (readOnly) {
      cmdArgs.add("--read-only");
    }
    if (ratis) {
      cmdArgs.add("--ratis");
    }

    int exitCode = cmd.execute(cmdArgs.toArray(new String[0]));
    assertEquals(0, exitCode);
  }
}
