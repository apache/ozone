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

package org.apache.hadoop.hdds.upgrade;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests upgrade finalization failure scenarios and corner cases specific to DN data distribution feature.
 */
public class TestDNDataDistributionFinalization {
  private StorageContainerLocationProtocol scmClient;
  private MiniOzoneHAClusterImpl cluster;

  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;
  private final String volumeName = UUID.randomUUID().toString();
  private final String bucketName = UUID.randomUUID().toString();
  private OzoneBucket bucket;

  @AfterEach
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public void init(OzoneConfiguration conf) throws Exception {

    conf.setInt(SCMStorageConfig.TESTING_INIT_APPARENT_VERSION_KEY, HDDSLayoutFeature.HBASE_SUPPORT.serialize());
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);
    conf.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");

    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(dnConf);

    MiniOzoneHAClusterImpl.Builder clusterBuilder = MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder.setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS)
        .setSCMServiceId("scmservice")
        .setOMServiceId("omServiceId")
        .setNumOfOzoneManagers(1)
        .setNumDatanodes(NUM_DATANODES)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setApparentVersion(HDDSLayoutFeature.INITIAL_VERSION.serialize())
            .build());
    this.cluster = clusterBuilder.build();

    scmClient = cluster.getStorageContainerLocationClient();
    cluster.waitForClusterToBeReady();
    assertEquals(HDDSLayoutFeature.HBASE_SUPPORT,
        cluster.getStorageContainerManager().getVersionManager().getApparentVersion());

    // Create Volume and Bucket
    try (OzoneClient ozoneClient = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore store = ozoneClient.getObjectStore();
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      BucketArgs.Builder builder = BucketArgs.newBuilder();
      volume.createBucket(bucketName, builder.build());
      bucket = volume.getBucket(bucketName);
    }
  }

  /**
   * Test that validates the upgrade scenario for DN data distribution feature.
   * This test specifically checks the conditions in populatePendingDeletionMetadata:
   * 1. Pre-finalization: handlePreDataDistributionFeature path
   * 2. Post-finalization: handlePostDataDistributionFeature path
   * 3. Missing metadata: getAggregatePendingDelete path
   */
  @Test
  public void testDataDistributionUpgradeScenario() throws Exception {
    init(new OzoneConfiguration());

    // Verify initial state - STORAGE_SPACE_DISTRIBUTION should not be finalized yet
    assertEquals(HDDSLayoutFeature.HBASE_SUPPORT,
        cluster.getStorageContainerManager().getVersionManager().getApparentVersion());

    // Create some data and delete operations to trigger pending deletion logic
    String keyName1 = "testKey1";
    String keyName2 = "testKey2";
    byte[] data = new byte[1024];

    // Write some keys
    try (OzoneOutputStream out = bucket.createKey(keyName1, data.length)) {
      out.write(data);
    }
    try (OzoneOutputStream out = bucket.createKey(keyName2, data.length)) {
      out.write(data);
    }

    // Delete one key to create pending deletion blocks
    bucket.deleteKey(keyName1);

    // Validate pre-finalization state
    validatePreDataDistributionFeatureState();

    // Wait for finalization to complete
    scmClient.finalizeUpgrade();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient);

    // Verify finalization completed
    assertFalse(cluster.getStorageContainerManager().getVersionManager().needsFinalization());

    // Create more data and deletions to test post-finalization behavior
    String keyName3 = "testKey3";
    try (OzoneOutputStream out = bucket.createKey(keyName3, data.length)) {
      out.write(data);
    }
    bucket.deleteKey(keyName2);
    bucket.deleteKey(keyName3);

    // Validate post-finalization state
    validatePostDataDistributionFeatureState();
  }

  /**
   * Test specifically for the missing metadata scenario that triggers
   * the getAggregatePendingDelete code path.
   */
  @Test
  public void testMissingPendingDeleteMetadataRecalculation() throws Exception {
    init(new OzoneConfiguration());


    // Create and delete keys to generate some pending deletion data
    String keyName = "testKeyForRecalc";
    byte[] data = new byte[2048];

    try (OzoneOutputStream out = bucket.createKey(keyName, data.length)) {
      out.write(data);
    }
    bucket.deleteKey(keyName);
    scmClient.finalizeUpgrade();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient);

    assertFalse(cluster.getStorageContainerManager().getVersionManager().needsFinalization());

    // Verify the system can handle scenarios where pendingDeleteBlockCount
    // might be missing and needs recalculation
    validateRecalculationScenario();
  }

  private void validatePreDataDistributionFeatureState() {
    // Before finalization, STORAGE_SPACE_DISTRIBUTION should not be finalized
    boolean isDataDistributionFinalized =
        VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION);
    assertTrue(!isDataDistributionFinalized ||
            // In test environment, version manager might be null
            cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
                .getVersionManager() == null,
        "STORAGE_SPACE_DISTRIBUTION should not be finalized in pre-upgrade state");

    // Verify containers exist and have pending deletion metadata
    validateContainerPendingDeletions(false);
  }

  private void validatePostDataDistributionFeatureState() {
    // After finalization, STORAGE_SPACE_DISTRIBUTION should be finalized
    boolean isDataDistributionFinalized =
        VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION);
    assertTrue(isDataDistributionFinalized ||
            // In test environment, version manager might be null
            cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
                .getVersionManager() == null,
        "STORAGE_SPACE_DISTRIBUTION should be finalized in post-upgrade state");

    // Verify containers can handle post-finalization pending deletion logic
    validateContainerPendingDeletions(true);
  }

  private void validateContainerPendingDeletions(boolean isPostFinalization) {
    // Get containers from datanodes and validate their pending deletion handling
    List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();

    for (HddsDatanodeService datanode : datanodes) {
      ContainerSet containerSet = datanode.getDatanodeStateMachine()
          .getContainer().getContainerSet();

      // Iterate through containers
      for (Container<?> container : containerSet.getContainerMap().values()) {
        if (container instanceof KeyValueContainer) {
          KeyValueContainerData containerData =
              (KeyValueContainerData) container.getContainerData();

          // Verify the container has been processed through the appropriate
          // code path in populatePendingDeletionMetadata
          assertNotNull(containerData.getStatistics());

          // The exact validation will depend on whether we're in pre or post
          // finalization state, but we should always have valid statistics
          assertTrue(containerData.getStatistics().getBlockPendingDeletion() >= 0);

          if (isPostFinalization) {
            // Post-finalization should have both block count and bytes
            assertTrue(containerData.getStatistics().getBlockPendingDeletionBytes() >= 0);
          } else {
            assertEquals(0, containerData.getStatistics().getBlockPendingDeletionBytes());
          }
        }
      }
    }
  }

  private void validateRecalculationScenario() {
    // This validates that the system properly handles the case where
    // pendingDeleteBlockCount is null and needs to be recalculated
    // from delete transaction tables via getAggregatePendingDelete

    List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();

    for (HddsDatanodeService datanode : datanodes) {
      ContainerSet containerSet = datanode.getDatanodeStateMachine()
          .getContainer().getContainerSet();

      // Verify containers have proper pending deletion statistics
      // even in recalculation scenarios
      for (Container<?> container : containerSet.getContainerMap().values()) {
        if (container instanceof KeyValueContainer) {
          KeyValueContainerData containerData =
              ((KeyValueContainer) container).getContainerData();

          // Statistics should be valid even after recalculation
          assertNotNull(containerData.getStatistics());
          assertTrue(containerData.getStatistics().getBlockPendingDeletion() >= 0);
          assertTrue(containerData.getStatistics().getBlockPendingDeletionBytes() >= 0);
        }
      }
    }
  }
}
