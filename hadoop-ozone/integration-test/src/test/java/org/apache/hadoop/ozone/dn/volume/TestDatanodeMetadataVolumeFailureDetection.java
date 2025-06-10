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

package org.apache.hadoop.ozone.dn.volume;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConsts.WITNESSED_CONTAINER_DB_NAME;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.dn.DatanodeTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This class tests datanode can detect failed volumes.
 */
class TestDatanodeMetadataVolumeFailureDetection {

  // witnessed db was introduced in schema V3 so we won't test for older schemas
  @ParameterizedTest
  @ValueSource(booleans = {true})
  void corruptDbFile(boolean schemaV3) throws Exception {
    try (MiniOzoneCluster cluster = newCluster(schemaV3)) {
      try (OzoneClient client = cluster.newClient()) {
        OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

        HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
        OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();

        File dbDir = null;
        if (schemaV3) {
          dbDir = Paths.get(ServerUtils.getOzoneMetaDirPath(cluster.getConf()).getAbsolutePath(),
                  "datanode-1", "meta", WITNESSED_CONTAINER_DB_NAME).toFile();
        }

        MutableVolumeSet metaVolumeSet = oc.getMetaVolumeSet();
        StorageVolume vol0 = metaVolumeSet.getVolumesList().get(0);

        try {
          // simulate a problem by removing the read permission of the db dir
          DatanodeTestUtils.injectContainerMetaDirFailure(dbDir);
          if (schemaV3) {
            // remove rocksDB from cache
            DatanodeStoreCache.getInstance().removeDB(dbDir.getAbsolutePath());
          }

          metaVolumeSet.checkVolumeAsync(vol0);
          DatanodeTestUtils.waitForCheckVolume(metaVolumeSet, 1L);
          DatanodeTestUtils.waitForHandleFailedVolume(metaVolumeSet, 1);
        } finally {
          // restore all
          DatanodeTestUtils.restoreContainerMetaDirFromFailure(dbDir);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true})
  void corruptDbFileWithoutDbHandleCacheInvalidation(boolean schemaV3) throws Exception {
    try (MiniOzoneCluster cluster = newCluster(schemaV3)) {
      try (OzoneClient client = cluster.newClient()) {
        OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

        HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
        OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();

        File dbDir = null;
        if (schemaV3) {
          dbDir = Paths.get(ServerUtils.getOzoneMetaDirPath(cluster.getConf()).getAbsolutePath(),
              "datanode-1", "meta", WITNESSED_CONTAINER_DB_NAME).toFile();
        }

        MutableVolumeSet metaVolumeSet = oc.getMetaVolumeSet();
        StorageVolume vol0 = metaVolumeSet.getVolumesList().get(0);

        try {
          // simulate a problem by removing the read permission of the db dir
          DatanodeTestUtils.injectContainerMetaDirFailure(dbDir);

          metaVolumeSet.checkVolumeAsync(vol0);
          DatanodeTestUtils.waitForCheckVolume(metaVolumeSet, 1L);
          DatanodeTestUtils.waitForHandleFailedVolume(metaVolumeSet, 1);
        } finally {
          // restore all
          DatanodeTestUtils.restoreContainerMetaDirFromFailure(dbDir);
        }
      }
    }
  }

  private static MiniOzoneCluster newCluster(boolean schemaV3)
      throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    ozoneConfig.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    ozoneConfig.setInt(OZONE_REPLICATION, 1);
    // keep the cache size = 1, so we could trigger io exception on
    // reading on-disk db instance
    ozoneConfig.setInt(OZONE_CONTAINER_CACHE_SIZE, 1);
    if (!schemaV3) {
      ContainerTestUtils.disableSchemaV3(ozoneConfig);
    }
    // set tolerated = 1
    // shorten the gap between successive checks to ease tests
    DatanodeConfiguration dnConf =
        ozoneConfig.getObject(DatanodeConfiguration.class);
    dnConf.setFailedDataVolumesTolerated(1);
    // We are corrupting the metadb volume in the tests. If toleration is set to the default value of 1,
    // the datanode will shut down and the test will exit without completing.
    // To avoid this, we increase the tolerated volume failures.
    dnConf.setFailedMetadataVolumesTolerated(10);
    dnConf.setDiskCheckMinGap(Duration.ofSeconds(2));
    ozoneConfig.setFromObject(dnConf);
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(ozoneConfig)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ReplicationFactor.ONE, 30000);

    return cluster;
  }
}
