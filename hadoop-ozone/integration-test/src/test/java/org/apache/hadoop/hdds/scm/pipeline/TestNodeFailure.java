/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Test Node failure detection and handling in Ratis.
 */
public class TestNodeFailure {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private static MiniOzoneCluster cluster;
  private static List<Pipeline> ratisPipelines;
  private static PipelineManager pipelineManager;
  private static int timeForFailure;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setFollowerSlownessTimeout(Duration.ofSeconds(10));
    ratisServerConfig.setNoLeaderTimeout(Duration.ofMinutes(5));
    conf.setFromObject(ratisServerConfig);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.set(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, "2s");

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(6)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .build();
    cluster.waitForClusterToBeReady();

    final StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
    ratisPipelines = pipelineManager.getPipelines(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE);

    timeForFailure = (int) ratisServerConfig
        .getFollowerSlownessTimeout();
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
  public void testPipelineFail() {
    ratisPipelines.forEach(pipeline -> {
      try {
        waitForPipelineCreation(pipeline.getId());
        cluster.shutdownHddsDatanode(pipeline.getFirstNode());
        GenericTestUtils.waitFor(() -> {
          try {
            return pipelineManager.getPipeline(pipeline.getId())
                .getPipelineState().equals(Pipeline.PipelineState.CLOSED);
          } catch (PipelineNotFoundException ex) {
            return true;
          }
        }, timeForFailure / 2, timeForFailure * 3);
      } catch (Exception e) {
        Assert.fail("Test Failed: " + e.getMessage());
      }
    });
  }

  /**
   * Waits until the Pipeline is marked as OPEN.
   * @param pipelineID Id of the pipeline
   */
  private void waitForPipelineCreation(final PipelineID pipelineID)
      throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        return pipelineManager.getPipeline(pipelineID)
            .getPipelineState().equals(Pipeline.PipelineState.OPEN);
      } catch (PipelineNotFoundException ex) {
        return false;
      }
    }, 1000, 1000 * 60);
  }
}
