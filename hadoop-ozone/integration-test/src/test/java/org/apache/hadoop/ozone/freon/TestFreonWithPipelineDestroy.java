/**
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

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Tests Freon with Pipeline destroy.
 */

public class TestFreonWithPipelineDestroy {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
            1, TimeUnit.SECONDS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL,
            1, TimeUnit.SECONDS);
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(3));
    conf.setFromObject(raftClientConfig);

    cluster = MiniOzoneCluster.newBuilder(conf)
      .setHbProcessorInterval(1000)
      .setHbInterval(1000)
      .setNumDatanodes(3)
      .setTotalPipelineNumLimit(8)
      .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRestart() throws Exception {
    startFreon();
    destroyPipeline();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
            180000);
    startFreon();
  }

  private void startFreon() throws Exception {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    randomKeyGenerator.setNumOfVolumes(1);
    randomKeyGenerator.setNumOfBuckets(1);
    randomKeyGenerator.setNumOfKeys(1);
    randomKeyGenerator.setType(ReplicationType.RATIS);
    randomKeyGenerator.setFactor(ReplicationFactor.THREE);
    randomKeyGenerator.setKeySize(20971520);
    randomKeyGenerator.setValidateWrites(true);
    randomKeyGenerator.call();
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(0,
        randomKeyGenerator.getUnsuccessfulValidationCount());
  }

  private void destroyPipeline() throws Exception {
    XceiverServerSpi server =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine().
            getContainer().getWriteChannel();
    StorageContainerDatanodeProtocolProtos.PipelineReport report =
        server.getPipelineReport().get(0);
    PipelineID id = PipelineID.getFromProtobuf(report.getPipelineID());
    PipelineManager pipelineManager =
        cluster.getStorageContainerManager().getPipelineManager();
    Pipeline pipeline = pipelineManager.getPipeline(id);
    pipelineManager.closePipeline(pipeline, false);
  }
}
