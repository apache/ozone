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

package org.apache.hadoop.ozone.container;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for container report handling with SCM High Availability.
 */
public class TestContainerReportHandlingWithHA {
  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";

  /**
   * Tests that a DELETING container moves to the CLOSED state if a non-empty CLOSED replica is reported. To do this,
   * the test first creates a key and closes its corresponding container. Then it moves that container to the
   * DELETING state using ContainerManager. Then it restarts a Datanode hosting that container, making it send a full
   * container report. Then the test waits for the container to move from DELETING to CLOSED in all SCMs.
   */
  @Test
  void testDeletingContainerTransitionsToClosedWhenNonEmptyReplicaIsReportedWithScmHA() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);

    int numSCM = 3;
    Path clusterPath = null;
    try (MiniOzoneHAClusterImpl cluster = newHACluster(conf, numSCM)) {
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

        // move the container to DELETING
        ContainerManager containerManager = cluster.getScmLeader().getContainerManager();
        containerManager.updateContainerState(containerID, HddsProtos.LifeCycleEvent.DELETE);
        assertEquals(HddsProtos.LifeCycleState.DELETING, containerManager.getContainer(containerID).getState());

        // restart a DN and wait for the container to get CLOSED in all SCMs
        HddsDatanodeService dn = cluster.getHddsDatanode(keyLocation.getPipeline().getFirstNode());
        cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
        ContainerManager[] array = new ContainerManager[numSCM];
        for (int i = 0; i < numSCM; i++) {
          array[i] = cluster.getStorageContainerManager(i).getContainerManager();
        }
        GenericTestUtils.waitFor(() -> {
          try {
            for (ContainerManager manager : array) {
              if (manager.getContainer(containerID).getState() != HddsProtos.LifeCycleState.CLOSED) {
                return false;
              }
            }
            return true;
          } catch (ContainerNotFoundException e) {
            fail(e);
          }
          return false;
        }, 2000, 20000);

        assertEquals(HddsProtos.LifeCycleState.CLOSED, containerManager.getContainer(containerID).getState());
      }
    } finally {
      if (clusterPath != null) {
        boolean deleted = FileUtil.fullyDelete(clusterPath.toFile());
        assertTrue(deleted);
      }
    }
  }

  private static MiniOzoneHAClusterImpl newHACluster(OzoneConfiguration conf, int numSCM) throws IOException {
    return MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service")
        .setSCMServiceId("scm-service")
        .setNumOfOzoneManagers(1)
        .setNumOfStorageContainerManagers(numSCM)
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

    try (OutputStream out = bucket.createKey(KEY, 0,
        RatisReplicationConfig.getInstance(THREE), emptyMap())) {
      out.write("Hello".getBytes(UTF_8));
    }
  }

}
