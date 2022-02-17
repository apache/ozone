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
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.protocol.commands.RefreshVolumeUsageCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_DU_FACTORY_CLASSNAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;

/**
 * Test the behaviour of the datanode and scm when communicating
 * with refresh volume usage command.
 */
public class TestRefreshVolumeUsageHandler {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;

  @Before
  public void setup() throws Exception {
    //setup a cluster (1G free space is enough for a unit test)
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    conf.set(HDDS_NODE_REPORT_INTERVAL, "1s");
    conf.set(HDDS_DATANODE_DU_FACTORY_CLASSNAME,
        "org.apache.hadoop.ozone.container.common.volume.HddsVolumeFactory");
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, 30000);
  }

  @After
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
    DatanodeUsageInfo usageInfo1 = cluster.getStorageContainerManager()
        .getScmNodeManager().getUsageInfo(datanodeDetails);

    //creating a key to take some storage space
    OzoneClient client = OzoneClientFactory.getRpcClient(conf);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume("test");
    objectStore.getVolume("test").createBucket("test");
    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey("test", 4096, ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    key.write("test".getBytes(UTF_8));
    key.close();

    //we set HDDS_NODE_REPORT_INTERVAL to 1s , let the cluster run for
    // 5s to enable several node report.
    Thread.sleep(5000L);

    //a new key is created , but the datanode default REFRESH_PERIOD is 1 hour,
    //so scm does not get the latest usage info of this datanode for now.
    Assert.assertTrue(
        usageInfo1.getScmNodeStat().getScmUsed().get().longValue() == 0);

    //send refresh volume usage command to datanode
    SCMCommand<?> command = new RefreshVolumeUsageCommand();
    command.setTerm(
        cluster.getStorageContainerManager().getScmContext().getTermOfLeader());
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getUuid(), command);

    //waiting for the new usage info is refreshed
    GenericTestUtils.waitFor(() ->
            isUsageInfoRefreshed(cluster, datanodeDetails),
            500,
            5 * 1000);
  }

  private static Boolean isUsageInfoRefreshed(MiniOzoneCluster cluster,
                                              DatanodeDetails datanodeDetails) {
    return cluster.getStorageContainerManager().getScmNodeManager()
        .getUsageInfo(datanodeDetails).getScmNodeStat()
        .getScmUsed().get().longValue() > 0L;
  }
}
