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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_GAP;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.recon.TestReconEndpointUtil.getReconWebAddress;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for the Storage Distribution REST endpoint in the
 * Recon service. This class is responsible for setting up the
 * testing environment, performing operations on an Ozone cluster,
 * and validating the response of the storage distribution endpoint.
 */
public class TestStorageDistributionEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStorageDistributionEndpoint.class);

  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster;
  private static OzoneManager om;
  private static StorageContainerManager scm;
  private static OzoneClient client;
  private static ReconService recon;
  private FileSystem fs;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String STORAGE_DIST_ENDPOINT = "/api/v1/storageDistribution";

  static List<Arguments> replicationConfigs() {
    return Collections.singletonList(
        Arguments.of(ReplicationConfig.fromTypeAndFactor(RATIS, THREE))
    );
  }

  @BeforeAll
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_TIMEOUT, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setLong(OZONE_SCM_HA_RATIS_SNAPSHOT_GAP, 1L);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 50,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);

    // Enhanced SCM configuration for faster block deletion processing
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);
    conf.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");

    // Enhanced DataNode configuration to move pending deletion from SCM to DN faster
    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionInterval(Duration.ofMillis(100));
    // Increase block delete queue limit to allow more queued commands on DN
    dnConf.setBlockDeleteQueueLimit(50);
    // Reduce the interval for delete command worker processing
    dnConf.setBlockDeleteCommandWorkerInterval(Duration.ofMillis(100));
    // Increase blocks deleted per interval to speed up deletion
    dnConf.setBlockDeletionLimit(5000);
    conf.setFromObject(dnConf);
    // Increase DN delete threads for faster parallel processing
    conf.setInt("ozone.datanode.block.delete.threads.max", 10);

    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    om = cluster.getOzoneManager();
    scm = cluster.getStorageContainerManager();
    client = cluster.newClient();
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  public void testStorageDistributionEndpoint(ReplicationConfig replicationConfig) throws Exception {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume("vol1");
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(replicationConfig))
        .build();

    OzoneVolume volume = objectStore.getVolume("vol1");
    volume.createBucket("bucket1", bucketArgs);
    OzoneBucket bucket = volume.getBucket("bucket1");

    String rootPath = String.format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucket.getName(),
        bucket.getVolumeName());

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);

    Path dir1 = new Path("/dir1");
    fs.mkdirs(dir1);
    for (int i = 1; i <= 10; i++) {
      Path path1 = new Path(dir1, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(path1)) {
        stream.write(1);
      }
    }
    Path dir2 = new Path("/dir2");
    fs.mkdirs(dir2);
    for (int i = 1; i <= 10; i++) {
      Path path1 = new Path(dir2, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(path1)) {
        stream.write(1);
      }
    }
    waitForKeysCreated(replicationConfig);
    Thread.sleep(10000);
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(getReconWebAddress(conf))
        .append(STORAGE_DIST_ENDPOINT);
    String response = TestReconEndpointUtil.makeHttpCall(conf, urlBuilder);
    StorageCapacityDistributionResponse storageResponse =
        MAPPER.readValue(response, StorageCapacityDistributionResponse.class);

    assertEquals(20, storageResponse.getGlobalNamespace().getTotalKeys());
    assertEquals(60, storageResponse.getGlobalNamespace().getTotalUsedSpace());
    assertEquals(0, storageResponse.getUsedSpaceBreakDown().getOpenKeyBytes());
    assertEquals(60, storageResponse.getUsedSpaceBreakDown().getCommittedKeyBytes());
    assertEquals(3, storageResponse.getDataNodeUsage().size());
  }

  private void verifyBlocksCreated(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws Exception {
    for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
      ContainerSet dnContainerSet =
          datanode.getDatanodeStateMachine().getContainer().getContainerSet();
      performOperationOnKeyContainers((blockID) -> {
        KeyValueContainerData cData = (KeyValueContainerData) dnContainerSet
            .getContainer(blockID.getContainerID()).getContainerData();
        try (DBHandle db = BlockUtils.getDB(cData, conf)) {
          assertNotNull(db.getStore().getBlockDataTable()
              .get(cData.getBlockKey(blockID.getLocalID())));
        }
      }, omKeyLocationInfoGroups);
    }
  }

  private static void syncDataFromOM() {
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
  }

  private void waitForKeysCreated(ReplicationConfig replicationConfig) throws Exception {
    for (int i = 1; i <= 10; i++) {
      OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName("vol1")
          .setBucketName("bucket1").setKeyName("dir1/testKey" + i).setDataSize(0)
          .setReplicationConfig(replicationConfig)
          .build();
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList =
          om.lookupKey(keyArgs).getKeyLocationVersions();

      // verify key blocks were created in DN.
      GenericTestUtils.waitFor(() -> {
        try {
          scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
          verifyBlocksCreated(omKeyLocationInfoGroupList);
          syncDataFromOM();
          return true;
        } catch (Throwable t) {
          LOG.warn("Verify blocks creation failed", t);
          return false;
        }
      }, 1000, 10000);
    }
  }

  private static void performOperationOnKeyContainers(
      CheckedConsumer<BlockID, Exception> consumer,
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws Exception {

    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup :
        omKeyLocationInfoGroups) {
      List<OmKeyLocationInfo> omKeyLocationInfos =
          omKeyLocationInfoGroup.getLocationList();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfos) {
        BlockID blockID = omKeyLocationInfo.getBlockID();
        consumer.accept(blockID);
      }
    }
  }

  @AfterEach
  public void cleanup() {
    assertDoesNotThrow(() -> {
      if (fs != null) {
        Path root = new Path("/");
        FileStatus[] fileStatuses = fs.listStatus(root);
        for (FileStatus fileStatus : fileStatuses) {
          fs.delete(fileStatus.getPath(), true);
        }
      }
    });
    IOUtils.closeQuietly(fs);
  }

  @AfterAll
  public static void tear() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
