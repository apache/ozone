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

package org.apache.hadoop.hdds.scm.pipeline;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test Node failure detection and handling in Ratis.
 */
public class TestNodeFailure {

  private static MiniOzoneCluster cluster;
  private static List<Pipeline> ratisPipelines;
  private static PipelineManager pipelineManager;
  private static int timeForFailure;

  private static final String FLOOD_TOKEN = "pipeline Action CLOSE";

  @BeforeAll
  public static void init() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setFollowerSlownessTimeout(Duration.ofSeconds(10));
    ratisServerConfig.setNoLeaderTimeout(Duration.ofMinutes(5));
    conf.setFromObject(ratisServerConfig);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.set(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, "2s");
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1000, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000, MILLISECONDS);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(6)
        .build();
    cluster.waitForClusterToBeReady();

    final StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
    ratisPipelines = pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE));

    timeForFailure = (int) ratisServerConfig
        .getFollowerSlownessTimeout();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPipelineFail() {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.captureLogs(XceiverServerRatis.class);
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
        fail("Test Failed: " + e.getMessage());
      }
    });
    logCapturer.stopCapturing();
    int occurrences = StringUtils.countMatches(logCapturer.getOutput(), FLOOD_TOKEN);
    assertThat(occurrences).isEqualTo(2);
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
