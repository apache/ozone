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

package org.apache.hadoop.ozone.container;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerStateInSCM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for container report handling.
 */
public class TestContainerReportHandling {
  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";

  /**
   * Tests that a DELETING (or DELETED) container moves to the CLOSED state if a non-empty replica is reported.
   * To do this, the test first creates a key and closes its corresponding container. Then it moves that container to
   * DELETING (or DELETED) state using ContainerManager. Then it restarts a Datanode hosting that container,
   * making it send a full container report.
   * the test waits for the container to move from DELETING to CLOSED.
   * the test waits for the replica to move from CLOSED to DELETED in SCM for DELETED container.
   */
  @ParameterizedTest
  @EnumSource(value = HddsProtos.LifeCycleState.class,
      names = {"DELETING", "DELETED"})
  void testDeletingOrDeletedContainerTransitionsToClosedWhenNonEmptyReplicaIsReported(
      HddsProtos.LifeCycleState desiredState)
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);

    Path clusterPath = null;
    try (MiniOzoneCluster cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();
      clusterPath = Paths.get(cluster.getBaseDir());

      try (OzoneClient client = cluster.newClient()) {
        // create a container and close it
        createTestData(client);
        List<OmKeyLocationInfo> keyLocations = lookupKey(cluster);
        assertThat(keyLocations).isNotEmpty();
        OmKeyLocationInfo keyLocation = keyLocations.get(0);
        ContainerID containerID = ContainerID.valueOf(keyLocation.getContainerID());
        waitForContainerClose(cluster, containerID.getId());

        // also wait till the container is closed in SCM
        waitForContainerStateInSCM(cluster.getStorageContainerManager(), containerID, HddsProtos.LifeCycleState.CLOSED);

        // move the container to DELETING
        ContainerManager containerManager = cluster.getStorageContainerManager().getContainerManager();
        assertTrue(containerManager.getContainerReplicas(containerID).size() > 0);
        containerManager.updateContainerState(containerID, HddsProtos.LifeCycleEvent.DELETE);
        assertEquals(HddsProtos.LifeCycleState.DELETING, containerManager.getContainer(containerID).getState());

        // move the container to DELETED in the second test case
        if (desiredState == HddsProtos.LifeCycleState.DELETED) {
          containerManager.updateContainerState(containerID, HddsProtos.LifeCycleEvent.CLEANUP);
          assertEquals(HddsProtos.LifeCycleState.DELETED, containerManager.getContainer(containerID).getState());
        }

        // restart all the DNs
        List<DatanodeDetails> dnlist = keyLocation.getPipeline().getNodes();
        for (DatanodeDetails dn: dnlist) {
          cluster.restartHddsDatanode(dn, false);
        }

        if (desiredState == HddsProtos.LifeCycleState.DELETING) {
          // wait for the container to get CLOSED in all SCMs
          waitForContainerStateInSCM(cluster.getStorageContainerManager(),
              containerID, HddsProtos.LifeCycleState.CLOSED);
          assertEquals(HddsProtos.LifeCycleState.CLOSED, containerManager.getContainer(containerID).getState());
        } else {
          // Since replica state is CLOSED and container is DELETED in SCM also bcsid of replica and container is same
          // SCM will trigger delete replica
          // wait for all replica gets deleted
          GenericTestUtils.waitFor(() -> {
            try {
              return containerManager.getContainerReplicas(containerID).isEmpty();
            } catch (ContainerNotFoundException e) {
              throw new RuntimeException(e);
            }
          }, 100, 120000);
          assertEquals(HddsProtos.LifeCycleState.DELETED, containerManager.getContainer(containerID).getState());

        }
      }
    } finally {
      if (clusterPath != null) {
        System.out.println("Deleting path " + clusterPath);
        boolean deleted = FileUtil.fullyDelete(clusterPath.toFile());
        assertTrue(deleted);
      }
    }
  }

  private static MiniOzoneCluster newCluster(OzoneConfiguration conf)
      throws IOException {
    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
  }

  private static List<OmKeyLocationInfo> lookupKey(MiniOzoneCluster cluster)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    assertNotNull(locations);
    return locations.getLocationList();
  }

  private void createTestData(OzoneClient client) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);

    OzoneBucket bucket = volume.getBucket(BUCKET);

    TestDataUtil.createKey(bucket, KEY,
        RatisReplicationConfig.getInstance(THREE), "Hello".getBytes(UTF_8));
  }

}
