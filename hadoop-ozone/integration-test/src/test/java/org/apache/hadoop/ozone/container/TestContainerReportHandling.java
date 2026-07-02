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
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.TestHelper.waitForContainerClose;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.TestHelper.ReplicationInput;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for container report handling.
 */
@ParameterizedClass
@MethodSource("clusters")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestContainerReportHandling {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final int DATANODE_COUNT = ReplicationInput.EC.getNumDatanodes();

  private static OzoneConfiguration conf;

  @Parameter
  private MiniOzoneCluster.Builder builder;

  private MiniOzoneCluster cluster;

  private static List<TestCase> delStatesAndReplication() {
    return Stream.of(
            LifeCycleState.DELETING,
            LifeCycleState.DELETED)
        .flatMap(state -> Stream.of(
            new TestCase(state, ReplicationInput.RATIS),
            new TestCase(state, ReplicationInput.EC)))
        .collect(Collectors.toList());
  }

  @BeforeAll
  static void createConf() {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
  }

  static Stream<MiniOzoneCluster.Builder> clusters() {
    return Stream.of(
        MiniOzoneCluster.newBuilder(conf),
        MiniOzoneCluster.newHABuilder(conf)
    );
  }

  @BeforeParameterizedClassInvocation
  void startCluster() throws Exception {
    cluster = builder.setNumDatanodes(DATANODE_COUNT).build();
    cluster.waitForClusterToBeReady();
  }

  @AfterParameterizedClassInvocation
  void shutdown() {
    Path clusterPath = Paths.get(cluster.getBaseDir());
    IOUtils.closeQuietly(cluster);
    assertTrue(FileUtil.fullyDelete(clusterPath.toFile()));
  }

  /**
   * Tests that a DELETING (or DELETED) container replica gets deleted when replica bcsid <= container bcsid
   * applicable to RATIS; EC ignores bcsid.
   * To do this, the test first creates a key and closes its corresponding container. Then it moves that container to
   * DELETING (or DELETED) state using ContainerManager. SCM then deletes the replicas when it processes a periodic
   * container report for the CLOSED replicas.
   * Tests wait for a DELETING (or DELETED) container replica gets deleted based on the bcsid comparison.
   */
  @Test
  void testDeletingOrDeletedContainerWhenNonEmptyReplicaIsReported() throws Exception {
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      objectStore.createVolume(VOLUME);
      OzoneVolume volume = objectStore.getVolume(VOLUME);
      volume.createBucket(BUCKET);
      OzoneBucket bucket = volume.getBucket(BUCKET);

      int keyCount = 0;

      for (TestCase testCase : delStatesAndReplication()) {
        LifeCycleState desiredState = testCase.getLeft();
        ReplicationInput replicationInput = testCase.getRight();
        // create a container and close it
        String key = "key" + keyCount;
        TestDataUtil.createKey(bucket, key, replicationInput.getReplicationConfig(), "Hello".getBytes(UTF_8));
        List<OmKeyLocationInfo> keyLocations = lookupKey(cluster, key);
        assertThat(keyLocations).isNotEmpty();
        OmKeyLocationInfo keyLocation = keyLocations.get(0);
        ContainerID containerID = ContainerID.valueOf(keyLocation.getContainerID());
        waitForContainerClose(cluster, containerID.getId());

        // also wait till the container is closed in SCM
        waitForContainerClosedInSCM(containerID);

        ContainerManager containerManager = cluster.getStorageContainerManager().getContainerManager();
        // Wait until SCM sees all replicas CLOSED before moving the container to DELETING. The container state above
        // flips to CLOSED as soon as the first replica is reported CLOSED, so a lagging replica may still be CLOSING in
        // SCM. Deleting then races with that lagging CLOSING report, which would resurrect the container out of
        // DELETING/DELETED and the replicas would never be deleted.
        TestHelper.waitForReplicaState(containerManager, containerID, replicationInput.getNumDatanodes(),
            ContainerReplicaProto.State.CLOSED);

        // move the container to DELETING
        assertFalse(containerManager.getContainerReplicas(containerID).isEmpty());
        containerManager.updateContainerState(containerID, HddsProtos.LifeCycleEvent.DELETE);
        assertEquals(LifeCycleState.DELETING, containerManager.getContainer(containerID).getState());

        // move the container to DELETED in the second test case
        if (desiredState == LifeCycleState.DELETED) {
          containerManager.updateContainerState(containerID, HddsProtos.LifeCycleEvent.CLEANUP);
          assertEquals(LifeCycleState.DELETED, containerManager.getContainer(containerID).getState());
        }

        // Since replica state is CLOSED and container is DELETED/DELETING in SCM, and the bcsid of replica and
        // container is same, SCM will trigger delete replica for RATIS (EC ignores bcsid) when it processes a
        // periodic container report for the CLOSED replicas.
        // wait for all replica to be deleted
        waitForAllReplicasDeleted(containerManager, containerID);
      }
    }
  }

  private void waitForContainerClosedInSCM(ContainerID containerID)
      throws TimeoutException, InterruptedException {
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      TestHelper.waitForContainerStateInSCM(scm, containerID, LifeCycleState.CLOSED);
    }
  }

  private static void waitForAllReplicasDeleted(ContainerManager containerManager, ContainerID containerID)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        return containerManager.getContainerReplicas(containerID).isEmpty();
      } catch (ContainerNotFoundException e) {
        throw new RuntimeException(e);
      }
    }, 100, 180000);
  }

  private static List<OmKeyLocationInfo> lookupKey(MiniOzoneCluster cluster, String key)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(key)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    assertNotNull(locations);
    return locations.getLocationList();
  }

  private static class TestCase extends ImmutablePair<LifeCycleState, ReplicationInput> {
    TestCase(LifeCycleState state, ReplicationInput replication) {
      super(state, replication);
    }
  }
}
