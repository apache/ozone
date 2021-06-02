/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertEquals;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.Assert.fail;

/**
 * Integration test to ensure a container can be closed and its replicas
 * reported back correctly after a SCM restart.
 */
public class TestCloseContainer {

  private static int numOfDatanodes = 3;
  private static String bucketName = "bucket1";
  private static String volName = "vol1";
  private OzoneBucket bucket;
  private MiniOzoneCluster cluster;

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        interval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    ReplicationManagerConfiguration replicationConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();

    bucket = TestDataUtil.createVolumeAndBucket(cluster, volName, bucketName);
  }

  @Test
  public void testReplicasAreReportedForClosedContainerAfterRestart()
      throws Exception {
    // Create some keys to write data into the open containers
    for (int i=0; i<10; i++) {
      TestDataUtil.createKey(bucket, "key"+i, ReplicationFactor.THREE,
          ReplicationType.RATIS, "this is the content");
    }
    StorageContainerManager scm = cluster.getStorageContainerManager();

    // Pick any container on the cluster, get its pipeline, close it and then
    // wait for the container to close
    ContainerInfo container = scm.getContainerManager().getContainers().get(0);
    Pipeline pipeline = scm.getPipelineManager()
        .getPipeline(container.getPipelineID());
    scm.getPipelineManager().closePipeline(pipeline, false);
    GenericTestUtils.waitFor(() ->
            container.getState() == HddsProtos.LifeCycleState.CLOSED,
        200, 30000);

    long originalSeq = container.getSequenceId();

    cluster.restartStorageContainerManager(true);

    scm = cluster.getStorageContainerManager();
    ContainerInfo newContainer
        = scm.getContainerManager().getContainer(container.containerID());

    // After restarting SCM, ensure the sequenceId for the container is the
    // same as before.
    assertEquals(originalSeq, newContainer.getSequenceId());

    // Ensure 3 replicas are reported successfully as expected.
    GenericTestUtils.waitFor(() ->
            getContainerReplicas(newContainer).size() == 3, 200, 30000);
  }

  /**
   * Retrieves the containerReplica set for a given container or fails the test
   * if the container cannot be found. This is a helper method to allow the
   * container replica count to be checked in a lambda expression.
   * @param c The container for which to retrieve replicas
   * @return
   */
  private Set<ContainerReplica> getContainerReplicas(ContainerInfo c) {
    Set<ContainerReplica> replicas = null;
    try {
      replicas = cluster.getStorageContainerManager()
          .getContainerManager().getContainerReplicas(c.containerID());
    } catch (ContainerNotFoundException e) {
      fail("Unexpected ContainerNotFoundException");
    }
    return replicas;
  }

}
