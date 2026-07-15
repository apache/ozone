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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.DUOptimized;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the behaviour of the datanode and scm when communicating
 * with refresh volume usage command.
 */
public class TestRefreshVolumeUsageHandler {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() throws Exception {
    //setup a cluster (1G free space is enough for a unit test)
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    conf.set(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE, "5GB");
    conf.set(HDDS_NODE_REPORT_INTERVAL, "1s");
    conf.set("hdds.datanode.du.factory.classname",
        "org.apache.hadoop.ozone.container.common.volume.HddsVolumeFactory");
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, 30000);
  }

  @AfterEach
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void test() throws Exception {
    cluster.waitForClusterToBeReady();
    DatanodeDetails datanodeDetails =
        cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    Long currentScmUsed = cluster.getStorageContainerManager()
        .getScmNodeManager().getUsageInfo(datanodeDetails)
        .getScmNodeStat().getScmUsed().get();

    GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer.captureLogs(DUOptimized.class);
    //creating a key to take some storage space
    try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore objectStore = client.getObjectStore();
      objectStore.createVolume("test");
      objectStore.getVolume("test").createBucket("test");
      OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
          .createKey("test", 4096, ReplicationType.RATIS,
              ReplicationFactor.ONE, new HashMap<>());
      key.write("test".getBytes(UTF_8));
      key.close();

      //a new key is created, but the datanode default REFRESH_PERIOD is 1 hour,
      //still the cache is updated, so the scm will eventually get the new
      //used space from the datanode through node report.
      assertTrue(cluster.getStorageContainerManager()
          .getScmNodeManager().getUsageInfo(datanodeDetails)
          .getScmNodeStat().getScmUsed().isEqual(currentScmUsed));

      try {
        GenericTestUtils.waitFor(() -> isUsageInfoRefreshed(cluster,
            datanodeDetails, currentScmUsed), 500, 5 * 1000);
      } catch (TimeoutException te) {
        //no op
      } catch (InterruptedException ie) {
        //no op
      }

      //after waiting for several node report , this usage info
      //in SCM should be updated as we have updated the DN's cached usage info.
      assertTrue(cluster.getStorageContainerManager()
          .getScmNodeManager().getUsageInfo(datanodeDetails)
          .getScmNodeStat().getScmUsed().isGreater(currentScmUsed));

      //send refresh volume usage command to datanode
      cluster.getStorageContainerManager()
          .getScmNodeManager().refreshAllHealthyDnUsageInfo();

      //waiting for the new usage info is refreshed
      GenericTestUtils.waitFor(() -> isUsageInfoRefreshed(cluster,
          datanodeDetails, currentScmUsed), 500, 5 * 1000);

      // force refresh and verify used space in optimized flow
      cluster.getHddsDatanodes().get(0).getDatanodeStateMachine().getContainer().getVolumeSet().getVolumesList().get(0)
          .getVolumeUsage().refreshNow();
      GenericTestUtils.waitFor(() -> logCapture.getOutput().contains("container data usages 4"), 500, 10 * 1000);
    }
  }

  private static Boolean isUsageInfoRefreshed(MiniOzoneCluster cluster,
                                              DatanodeDetails datanodeDetails,
                                              long currentScmUsed) {
    return cluster.getStorageContainerManager().getScmNodeManager()
      .getUsageInfo(datanodeDetails).getScmNodeStat()
      .getScmUsed().isGreater(currentScmUsed);
  }
}
