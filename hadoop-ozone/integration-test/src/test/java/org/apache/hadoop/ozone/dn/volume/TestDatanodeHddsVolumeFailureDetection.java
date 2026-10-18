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

import static org.apache.commons.io.IOUtils.readFully;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.dn.DatanodeTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This class tests datanode can detect failed volumes.
 */
@ParameterizedClass
@ValueSource(booleans = {true, false})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDatanodeHddsVolumeFailureDetection {
  private static final int KEY_SIZE = 128;

  @Parameter
  private boolean schemaV3;

  private MiniOzoneCluster cluster;
  private HddsDatanodeService currentDatanode;
  private long currentContainerId;

  @BeforeParameterizedClassInvocation
  void initCluster() throws Exception {
    cluster = newCluster(schemaV3);
  }

  @AfterEach
  void failCurrentVolume() throws Exception {
    HddsDatanodeService datanode = currentDatanode;
    currentDatanode = null;
    if (datanode == null) {
      return;
    }
    cluster.getStorageContainerLocationClient().closeContainer(currentContainerId);
    OzoneContainer container = datanode.getDatanodeStateMachine().getContainer();
    MutableVolumeSet volumeSet = container.getVolumeSet();
    if (!volumeSet.getVolumesList().isEmpty()) {
      StorageVolume volume = volumeSet.getVolumesList().get(0);
      volumeSet.failVolume(volume.getStorageDir().getPath());
      container.handleVolumeFailures();
    }
    waitForHandleFailedVolume(volumeSet);
    GenericTestUtils.waitFor(() -> isFailedVolumeReported(datanode), 100, 10000);
  }

  @AfterParameterizedClassInvocation
  void shutdown() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void corruptChunkFile() throws Exception {
    try (OzoneClient client = cluster.newClient()) {
      OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client);

      // write a file
      String keyName = UUID.randomUUID().toString();
      long containerId = createKey(bucket, keyName);
      currentContainerId = containerId;

      // corrupt chunk file by rename file->dir
      HddsDatanodeService dn = getDatanode(containerId);
      currentDatanode = dn;
      OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
      MutableVolumeSet volSet = oc.getVolumeSet();
      StorageVolume vol0 = volSet.getVolumesList().get(0);
      HddsVolume volume = assertInstanceOf(HddsVolume.class, vol0);
      Path chunksPath = Paths.get(
          volume.getStorageDir().getPath(),
          volume.getClusterID(),
          Storage.STORAGE_DIR_CURRENT,
          Storage.CONTAINER_DIR + "0",
          String.valueOf(containerId),
          OzoneConsts.STORAGE_DIR_CHUNKS
      );
      File[] chunkFiles = chunksPath.toFile().listFiles();
      assertNotNull(chunkFiles);

      try {
        for (File chunkFile : chunkFiles) {
          DatanodeTestUtils.injectDataFileFailure(chunkFile);
        }

        // simulate bad volume by removing write permission on root dir
        // refer to HddsVolume.check()
        DatanodeTestUtils.simulateBadVolume(vol0);

        // read written file to trigger checkVolumeAsync
        readKeyToTriggerCheckVolumeAsync(bucket, keyName);

        // should trigger checkVolumeAsync and
        // a failed volume should be detected
        waitForHandleFailedVolume(volSet);
      } finally {
        // restore for cleanup
        DatanodeTestUtils.restoreBadVolume(vol0);
        for (File chunkFile : chunkFiles) {
          DatanodeTestUtils.restoreDataFileFromFailure(chunkFile);
        }
      }
    }
  }

  @Test
  void corruptContainerFile() throws Exception {
    // create a container
    ContainerWithPipeline container;
    OzoneConfiguration conf = cluster.getConf();
    try (ScmClient scmClient = new ContainerOperationClient(conf)) {
      container = scmClient.createContainer(
          RatisReplicationConfig.getInstance(ReplicationFactor.ONE),
          OzoneConsts.OZONE);
    }
    currentContainerId = container.getContainerInfo().getContainerID();

    // corrupt container file by removing write permission on
    // container metadata dir, since container update operation
    // use a create temp & rename way, so we can't just rename
    // container file to simulate corruption
    HddsDatanodeService dn = cluster.getHddsDatanode(container.getPipeline().getFirstNode());
    currentDatanode = dn;
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    MutableVolumeSet volSet = oc.getVolumeSet();
    StorageVolume vol0 = volSet.getVolumesList().get(0);
    Container<?> c1 = oc.getContainerSet().getContainer(
        container.getContainerInfo().getContainerID());
    File metadataDir = new File(c1.getContainerFile().getParent());
    try {
      DatanodeTestUtils.injectContainerMetaDirFailure(metadataDir);

      // simulate bad volume by removing write permission on root dir
      // refer to HddsVolume.check()
      DatanodeTestUtils.simulateBadVolume(vol0);

      // close container to trigger checkVolumeAsync
      assertThrows(IOException.class, c1::close);

      // should trigger CheckVolumeAsync and
      // a failed volume should be detected
      waitForHandleFailedVolume(volSet);
    } finally {
      // restore for cleanup
      DatanodeTestUtils.restoreBadVolume(vol0);
      DatanodeTestUtils.restoreContainerMetaDirFromFailure(metadataDir);
    }
  }

  @Test
  void corruptDbFile() throws Exception {
    try (OzoneClient client = cluster.newClient()) {
      OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client);

      // write a file, will create container1
      String keyName = UUID.randomUUID().toString();
      long containerId = createKey(bucket, keyName);
      currentContainerId = containerId;

      // close container1
      HddsDatanodeService dn = getDatanode(containerId);
      currentDatanode = dn;
      OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
      Container<?> c1 = oc.getContainerSet().getContainer(containerId);
      c1.close();

      // corrupt db by rename dir->file
      File dbDir;
      if (schemaV3) {
        dbDir = new File(((KeyValueContainerData) (c1.getContainerData()))
            .getDbFile().getAbsolutePath());
      } else {
        File metadataDir = new File(c1.getContainerFile().getParent());
        dbDir = new File(metadataDir, containerId + OzoneConsts.DN_CONTAINER_DB);
      }

      MutableVolumeSet volSet = oc.getVolumeSet();
      StorageVolume vol0 = volSet.getVolumesList().get(0);

      try {
        // remove RocksDB from cache
        KeyValueContainerData containerData = (KeyValueContainerData) c1.getContainerData();
        if (schemaV3) {
          DatanodeStoreCache.getInstance().removeDB(dbDir.getAbsolutePath());
        } else {
          BlockUtils.removeDB(containerData, cluster.getConf());
        }
        DatanodeTestUtils.injectDataDirFailure(dbDir);

        // simulate bad volume by removing write permission on root dir
        // refer to HddsVolume.check()
        DatanodeTestUtils.simulateBadVolume(vol0);

        readKeyToTriggerCheckVolumeAsync(bucket, keyName);

        // should trigger CheckVolumeAsync and
        // a failed volume should be detected
        waitForHandleFailedVolume(volSet);
      } finally {
        // restore all
        DatanodeTestUtils.restoreBadVolume(vol0);
        DatanodeTestUtils.restoreDataDirFromFailure(dbDir);
      }
    }
  }

  /**
   * {@link HddsVolume#check(Boolean)} will capture the failures injected by this test and not allow the
   * test to reach the helper method {@link HddsVolume#checkDbHealth}.
   * As a workaround, we test the helper method directly.
   * As we test the helper method directly, we cannot test for schemas older than V3.
   */
  @Test
  void corruptDbFileWithoutDbHandleCacheInvalidation() throws Exception {
    assumeTrue(schemaV3);
    try (OzoneClient client = cluster.newClient()) {
      OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client);

      // write a file, will create container1
      String keyName = UUID.randomUUID().toString();
      long containerId = createKey(bucket, keyName);
      currentContainerId = containerId;

      // close container1
      HddsDatanodeService dn = getDatanode(containerId);
      currentDatanode = dn;
      OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
      Container<?> c1 = oc.getContainerSet().getContainer(containerId);
      c1.close();

      // corrupt db by rename dir->file
      File dbDir = new File(((KeyValueContainerData) (c1.getContainerData()))
          .getDbFile().getAbsolutePath());

      MutableVolumeSet volSet = oc.getVolumeSet();
      HddsVolume vol0 = (HddsVolume) volSet.getVolumesList().get(0);

      try {
        DatanodeTestUtils.injectDataDirFailure(dbDir);
        // simulate bad volume by removing write permission on root dir
        // refer to HddsVolume.check()
        DatanodeTestUtils.simulateBadVolume(vol0);

        // one volume health check got automatically executed when the cluster started
        // the second health should log the rocksdb failure but return a healthy-volume status
        assertEquals(VolumeCheckResult.HEALTHY, vol0.checkDbHealth(dbDir));
        // the third health check should log the rocksdb failure and return a failed-volume status
        assertEquals(VolumeCheckResult.FAILED, vol0.checkDbHealth(dbDir));
      } finally {
        // restore all
        DatanodeTestUtils.restoreBadVolume(vol0);
        DatanodeTestUtils.restoreDataDirFromFailure(dbDir);
      }
    }
  }

  private static void readKeyToTriggerCheckVolumeAsync(OzoneBucket bucket,
      String key) throws IOException {
    try (InputStream is = bucket.readKey(key)) {
      assertThrows(IOException.class, () -> readFully(is, new byte[KEY_SIZE]));
    }
  }

  private HddsDatanodeService getDatanode(long containerId) throws IOException {
    try (ScmClient scmClient = new ContainerOperationClient(cluster.getConf())) {
      return cluster.getHddsDatanode(scmClient.getContainerWithPipeline(containerId)
          .getPipeline().getFirstNode());
    }
  }

  private boolean isFailedVolumeReported(HddsDatanodeService datanode) {
    DatanodeInfo datanodeInfo = cluster.getStorageContainerManager().getScmNodeManager()
        .getNode(datanode.getDatanodeDetails().getID());
    return datanodeInfo != null && datanodeInfo.getFailedVolumeCount() == 1;
  }

  private static void waitForHandleFailedVolume(MutableVolumeSet volumeSet) throws Exception {
    DatanodeTestUtils.waitForHandleFailedVolume(volumeSet, 1);
    GenericTestUtils.waitFor(() -> volumeSet.getVolumesList().isEmpty(), 100, 10000);
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
    ozoneConfig.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 100, TimeUnit.MILLISECONDS);
    ozoneConfig.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 100, TimeUnit.MILLISECONDS);
    ozoneConfig.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100, TimeUnit.MILLISECONDS);
    ozoneConfig.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    ozoneConfig.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    if (!schemaV3) {
      ContainerTestUtils.disableSchemaV3(ozoneConfig);
    }
    // set tolerated = 1
    // shorten the gap between successive checks to ease tests
    DatanodeConfiguration dnConf =
        ozoneConfig.getObject(DatanodeConfiguration.class);
    dnConf.setFailedDataVolumesTolerated(1);
    dnConf.setDiskCheckMinGap(Duration.ofSeconds(0));
    ozoneConfig.setFromObject(dnConf);
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(ozoneConfig)
        .setNumDatanodes(4)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ReplicationFactor.ONE, 30000);

    return cluster;
  }

  private static long createKey(OzoneBucket bucket, String key)
      throws IOException {
    byte[] bytes = RandomUtils.secure().randomBytes(KEY_SIZE);
    RatisReplicationConfig replication =
        RatisReplicationConfig.getInstance(ReplicationFactor.ONE);
    DataTestUtil.createKey(bucket, key, replication, bytes);
    OzoneKeyDetails keyDetails = bucket.getKey(key);
    assertEquals(key, keyDetails.getName());
    return keyDetails.getOzoneKeyLocations().get(0).getContainerID();
  }

}
