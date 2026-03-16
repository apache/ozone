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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.OzoneStoragePolicy;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test verifying that storage policy (HOT/WARM/COLD) is correctly
 * threaded from OM to DN, so that container replicas land on the right volume
 * type (SSD/DISK/ARCHIVE).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestStoragePolicyPlacement {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStoragePolicyPlacement.class);

  private static final int NUM_DATANODES = 5;

  private MiniOzoneCluster cluster;
  private OzoneClient client;

  /**
   * A DatanodeFactory that delegates to {@link UniformDatanodesFactory} for
   * all standard setup (ports, metadata, ratis dirs, etc.) but replaces the
   * data dirs with storage-type-annotated paths.
   */
  private static MiniOzoneCluster.DatanodeFactory typedVolumeFactory(
      String... storageTypes) {
    UniformDatanodesFactory base = UniformDatanodesFactory.newBuilder()
        .setNumDataVolumes(storageTypes.length)
        .build();
    return conf -> {
      OzoneConfiguration dnConf = base.apply(conf);
      // UniformDatanodesFactory already created the data dirs.
      // Replace them with storage-type-annotated dirs.
      String existingDirs = dnConf.get(HDDS_DATANODE_DIR_KEY);
      String[] dirs = existingDirs.split(",");
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < dirs.length && i < storageTypes.length; i++) {
        if (sb.length() > 0) {
          sb.append(',');
        }
        sb.append('[').append(storageTypes[i]).append(']').append(dirs[i]);
      }
      dnConf.set(HDDS_DATANODE_DIR_KEY, sb.toString());
      return dnConf;
    };
  }

  @BeforeAll
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE, true);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DATANODES)
        .setDatanodeFactory(typedVolumeFactory("SSD", "DISK"))
        .build();
    cluster.waitForClusterToBeReady();

    // Wait for pipelines to form (the background creator may take a few
    // seconds to discover SSD-capable nodes and create pipelines).
    waitForPipelines(30, TimeUnit.SECONDS);

    client = cluster.newClient();
  }

  @AfterAll
  void teardown() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("Error closing client", e);
      }
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * HOT-policy bucket writes should place container replicas on SSD volumes.
   */
  @Test
  void testHotPolicyWritesLandOnSSD() throws Exception {
    String volumeName = "vol-" + UUID.randomUUID();
    String bucketName = "bkt-hot";
    String keyName = "key-hot";

    ObjectStore store = client.getObjectStore();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName,
        BucketArgs.newBuilder()
            .setStoragePolicy(OzoneStoragePolicy.HOT)
            .build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Write a small key
    byte[] data = "hello-hot-policy".getBytes(StandardCharsets.UTF_8);
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length)) {
      out.write(data);
    }

    // Look up the key to find container locations
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    assertNotNull(keyInfo);
    List<OmKeyLocationInfo> locations =
        keyInfo.getLatestVersionLocations().getLocationList();
    assertFalse(locations.isEmpty(), "Key should have at least one block");

    // For each block, check that the container replica is on an SSD volume
    for (OmKeyLocationInfo loc : locations) {
      long containerId = loc.getContainerID();
      LOG.info("Checking container {} for SSD placement", containerId);
      assertContainerOnVolumeType(containerId, StorageType.SSD);
    }
  }

  /**
   * When no SSD volumes exist, HOT-policy writes should fall back to DISK.
   */
  @Test
  void testHotPolicyFallsBackToDiskWhenNoSSD() throws Exception {
    // Use a separate mini cluster with DISK-only volumes
    OzoneConfiguration diskOnlyConf = new OzoneConfiguration();
    diskOnlyConf.setBoolean(
        OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE, true);

    MiniOzoneCluster diskCluster = MiniOzoneCluster.newBuilder(diskOnlyConf)
        .setNumDatanodes(3)
        .setDatanodeFactory(typedVolumeFactory("DISK"))
        .build();
    try {
      diskCluster.waitForClusterToBeReady();

      try (OzoneClient diskClient = diskCluster.newClient()) {
        String volumeName = "vol-" + UUID.randomUUID();
        String bucketName = "bkt-fallback";
        String keyName = "key-fallback";

        ObjectStore store = diskClient.getObjectStore();
        store.createVolume(volumeName);
        OzoneVolume volume = store.getVolume(volumeName);
        volume.createBucket(bucketName,
            BucketArgs.newBuilder()
                .setStoragePolicy(OzoneStoragePolicy.HOT)
                .build());
        OzoneBucket bucket = volume.getBucket(bucketName);

        // Write should succeed even though no SSD volumes exist
        byte[] data = "hello-fallback".getBytes(StandardCharsets.UTF_8);
        try (OzoneOutputStream out = bucket.createKey(keyName, data.length)) {
          out.write(data);
        }

        // Verify key is readable
        byte[] readBuf = new byte[data.length];
        try (java.io.InputStream in = bucket.readKey(keyName)) {
          int bytesRead = in.read(readBuf);
          assertEquals(data.length, bytesRead);
        }

        // Verify replicas are on DISK volumes
        OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .build();
        OmKeyInfo keyInfo = diskCluster.getOzoneManager().lookupKey(keyArgs);
        assertNotNull(keyInfo);
        List<OmKeyLocationInfo> locations =
            keyInfo.getLatestVersionLocations().getLocationList();
        assertFalse(locations.isEmpty());

        for (OmKeyLocationInfo loc : locations) {
          long containerId = loc.getContainerID();
          assertContainerOnVolumeType(
              diskCluster, containerId, StorageType.DISK);
        }
      }
    } finally {
      diskCluster.shutdown();
    }
  }

  /**
   * End-to-end test: when a container created on SSD (HOT policy) is
   * under-replicated and replication manager creates a new replica,
   * that new replica should also land on an SSD volume.
   */
  @Test
  void testReplicatedContainerLandsOnCorrectVolumeType() throws Exception {
    OzoneConfiguration repConf = new OzoneConfiguration();
    repConf.setBoolean(OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE, true);
    // Aggressive intervals for fast detection (matching
    // TestReplicationManagerIntegration pattern)
    repConf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    repConf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        1, TimeUnit.SECONDS);
    repConf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL,
        1, TimeUnit.SECONDS);
    repConf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL,
        1, TimeUnit.SECONDS);
    repConf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL,
        1, TimeUnit.SECONDS);
    repConf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL,
        1, TimeUnit.SECONDS);
    repConf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);
    repConf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL,
        6, TimeUnit.SECONDS);
    repConf.setTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);
    // Fast replication manager via typed config object
    ReplicationManagerConfiguration rmConf =
        repConf.getObject(ReplicationManagerConfiguration.class);
    rmConf.setInterval(Duration.ofSeconds(1));
    rmConf.setUnderReplicatedInterval(Duration.ofMillis(500));
    repConf.setFromObject(rmConf);

    // 4 datanodes: Ratis THREE needs 3 active + 1 for replication target
    MiniOzoneCluster repCluster = MiniOzoneCluster.newBuilder(repConf)
        .setNumDatanodes(4)
        .setDatanodeFactory(typedVolumeFactory("SSD", "DISK"))
        .build();
    try {
      repCluster.waitForClusterToBeReady();
      waitForPipelines(repCluster, 30, TimeUnit.SECONDS);

      try (OzoneClient repClient = repCluster.newClient()) {
        String volumeName = "vol-" + UUID.randomUUID();
        String bucketName = "bkt-rep";
        String keyName = "key-rep";

        ObjectStore store = repClient.getObjectStore();
        store.createVolume(volumeName);
        OzoneVolume volume = store.getVolume(volumeName);
        volume.createBucket(bucketName,
            BucketArgs.newBuilder()
                .setStoragePolicy(OzoneStoragePolicy.HOT)
                .build());
        OzoneBucket bucket = volume.getBucket(bucketName);

        // Write a key — containers created on SSD
        byte[] data = "hello-replication-test".getBytes(StandardCharsets.UTF_8);
        try (OzoneOutputStream out = bucket.createKey(keyName, data.length)) {
          out.write(data);
        }

        // Look up the key to find container locations
        OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .build();
        OmKeyInfo keyInfo =
            repCluster.getOzoneManager().lookupKey(keyArgs);
        assertNotNull(keyInfo);
        List<OmKeyLocationInfo> locations =
            keyInfo.getLatestVersionLocations().getLocationList();
        assertFalse(locations.isEmpty(),
            "Key should have at least one block");

        long containerIdVal = locations.get(0).getContainerID();
        LOG.info("Container {} written with HOT policy", containerIdVal);

        // Verify initial replicas are on SSD
        assertContainerOnVolumeType(
            repCluster, containerIdVal, StorageType.SSD);

        closeAndTriggerReplication(repCluster, containerIdVal);

        // Verify ALL current replicas (including new ones) are on SSD
        assertContainerOnVolumeType(
            repCluster, containerIdVal, StorageType.SSD);
      }
    } finally {
      repCluster.shutdown();
    }
  }

  /**
   * Close a container through the proper pipeline-close flow, then stop
   * one datanode to trigger under-replication and wait for the replication
   * manager to create a new replica.
   */
  private void closeAndTriggerReplication(
      MiniOzoneCluster repCluster, long containerIdVal) throws Exception {
    ContainerManager cm = repCluster.getStorageContainerManager()
        .getContainerManager();
    ContainerID containerId = ContainerID.valueOf(containerIdVal);
    ContainerInfo containerInfo = cm.getContainer(containerId);

    // Close via pipeline close — triggers proper DN close + report flow
    OzoneTestUtils.closeContainer(
        repCluster.getStorageContainerManager(), containerInfo);

    Set<ContainerReplica> replicas = cm.getContainerReplicas(containerId);
    assertEquals(3, replicas.size(), "Ratis THREE should have 3 replicas");
    DatanodeDetails dnToStop =
        replicas.iterator().next().getDatanodeDetails();
    LOG.info("Shutting down datanode {} to trigger under-replication",
        dnToStop.getUuidString());
    repCluster.shutdownHddsDatanode(dnToStop);

    GenericTestUtils.waitFor(() -> {
      try {
        long healthyCount = cm.getContainerReplicas(containerId).stream()
            .filter(r -> !r.getDatanodeDetails().equals(dnToStop))
            .count();
        return healthyCount >= 3;
      } catch (Exception e) {
        return false;
      }
    }, 2000, 120000);

    assertTrue(cm.getContainerReplicas(containerId).size() >= 3,
        "Expected at least 3 replicas");
  }

  /**
   * Assert that a container's replicas on the datanodes in {@code this.cluster}
   * are placed on volumes of the expected type.
   */
  private void assertContainerOnVolumeType(
      long containerId, StorageType expectedType) throws Exception {
    assertContainerOnVolumeType(cluster, containerId, expectedType);
  }

  /**
   * Assert that a container's replicas on the datanodes in the given cluster
   * are placed on volumes of the expected type.
   */
  private void assertContainerOnVolumeType(
      MiniOzoneCluster miniCluster, long containerId,
      StorageType expectedType) throws Exception {
    ContainerManager cm =
        miniCluster.getStorageContainerManager().getContainerManager();
    Set<ContainerReplica> replicas =
        cm.getContainerReplicas(ContainerID.valueOf(containerId));
    assertFalse(replicas.isEmpty(),
        "Container " + containerId + " should have replicas");

    for (ContainerReplica replica : replicas) {
      DatanodeDetails dn = replica.getDatanodeDetails();
      HddsDatanodeService dnService = findDatanode(miniCluster, dn);
      if (dnService == null) {
        LOG.info("Skipping stopped datanode {}", dn.getUuidString());
        continue;
      }

      ContainerSet containerSet = dnService.getDatanodeStateMachine()
          .getContainer().getContainerSet();
      Container<?> container = containerSet.getContainer(containerId);
      if (container == null) {
        continue;
      }

      ContainerData containerData = container.getContainerData();
      HddsVolume vol = containerData.getVolume();
      assertNotNull(vol, "Container volume is null");

      LOG.info("Container {} on DN {} is on volume {} (type={})",
          containerId, dn.getUuidString(),
          vol.getStorageDir(), vol.getStorageType());
      assertEquals(expectedType.name(), vol.getStorageType().name(),
          "Container " + containerId + " on DN " + dn.getUuidString()
              + " expected volume type " + expectedType
              + " but found " + vol.getStorageType());
    }
  }

  private HddsDatanodeService findDatanode(
      MiniOzoneCluster miniCluster, DatanodeDetails target) {
    for (HddsDatanodeService dn : miniCluster.getHddsDatanodes()) {
      if (dn.getDatanodeDetails().getUuid().equals(target.getUuid())) {
        return dn;
      }
    }
    return null;
  }

  /**
   * Wait until at least one open pipeline exists in the given cluster.
   */
  private static void waitForPipelines(
      MiniOzoneCluster miniCluster,
      long timeout, TimeUnit unit) throws Exception {
    long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < deadline) {
      boolean found = miniCluster.getStorageContainerManager()
          .getPipelineManager()
          .getPipelines()
          .stream()
          .anyMatch(p -> p.isOpen() && p.getNodes().size() >= 3);
      if (found) {
        LOG.info("Found open pipeline(s) -- proceeding with tests");
        return;
      }
      Thread.sleep(1000);
    }
    LOG.warn("Timed out waiting for pipelines -- "
        + "tests may still pass if fallback works");
  }

  /**
   * Wait until at least one open pipeline exists.
   */
  private void waitForPipelines(
      long timeout, TimeUnit unit) throws Exception {
    long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
    while (System.currentTimeMillis() < deadline) {
      boolean found = cluster.getStorageContainerManager()
          .getPipelineManager()
          .getPipelines()
          .stream()
          .anyMatch(p -> p.isOpen() && p.getNodes().size() >= 3);
      if (found) {
        LOG.info("Found open pipeline(s) -- proceeding with tests");
        return;
      }
      Thread.sleep(1000);
    }
    LOG.warn("Timed out waiting for pipelines -- "
        + "tests may still pass if fallback works");
  }
}
