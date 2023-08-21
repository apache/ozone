/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.recon;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Rule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

/**
 * Test Ozone Recon.
 */
public class TestReconWithOzoneManagerFSO {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReconWithOzoneManagerFSO.class);

  private static OzoneClient client;
  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static ObjectStore store;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED);
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 60000);
    conf.setInt(OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK, 60000);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 60000,
        TimeUnit.MILLISECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf)
                    .setNumDatanodes(1)
                    .includeRecon(true)
                    .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);

    client = cluster.newClient();
    store = client.getObjectStore();
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void writeTestData(String volumeName,
                             String bucketName,
                             String keyName) throws Exception {

    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
            keyString, 100).getBytes(UTF_8);
    OzoneOutputStream keyStream = TestHelper.createKey(
            keyName, ReplicationType.RATIS, ReplicationFactor.ONE,
            100, store, volumeName, bucketName);
    keyStream.write(data);
    keyStream.close();
  }

  private void writeKeys(String vol, String bucket, String key)
          throws Exception {
    store.createVolume(vol);
    OzoneVolume volume = store.getVolume(vol);
    volume.createBucket(bucket);
    writeTestData(vol, bucket, key);
  }

  public void deleteKey(String vol, String bucket, String key)
      throws Exception {
    OzoneVolume volume = store.getVolume(vol);
    OzoneBucket ozoneBucket = volume.getBucket(bucket);
    ozoneBucket.deleteKey(key);
  }

  public void deleteDirectory(String vol, String bucket, String dirName)
      throws Exception {
    OzoneVolume volume = store.getVolume(vol);
    OzoneBucket ozoneBucket = volume.getBucket(bucket);
    ozoneBucket.deleteDirectory(dirName, true);
  }

  @Test
  public void testNamespaceSummaryAPI() throws Exception {
    // add a vol, bucket and key
    addKeys(0, 10, "dir");
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
            cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
    ReconNamespaceSummaryManager namespaceSummaryManager =
            cluster.getReconServer().getReconNamespaceSummaryManager();
    ReconOMMetadataManager omMetadataManagerInstance =
            (ReconOMMetadataManager)
                    cluster.getReconServer().getOzoneManagerServiceProvider()
                            .getOMMetadataManagerInstance();
    OzoneStorageContainerManager reconSCM =
            cluster.getReconServer().getReconStorageContainerManager();
    NSSummaryEndpoint endpoint = new NSSummaryEndpoint(namespaceSummaryManager,
            omMetadataManagerInstance, reconSCM);
    Response basicInfo = endpoint.getBasicInfo("/vol1/bucket1/dir1");
    NamespaceSummaryResponse entity =
            (NamespaceSummaryResponse) basicInfo.getEntity();
    Assert.assertSame(entity.getEntityType(), EntityType.DIRECTORY);
    Assert.assertEquals(1, entity.getCountStats().getNumTotalKey());
    Assert.assertEquals(0, entity.getCountStats().getNumTotalDir());
    Assert.assertEquals(-1, entity.getCountStats().getNumVolume());
    Assert.assertEquals(-1, entity.getCountStats().getNumBucket());
    for (int i = 0; i < 10; i++) {
      Assert.assertNotNull(impl.getOMMetadataManagerInstance()
              .getVolumeTable().get("/vol" + i));
    }
    addKeys(10, 12, "dir");
    impl.syncDataFromOM();

    // test Recon is sync'ed with OM.
    for (int i = 10; i < 12; i++) {
      Assert.assertNotNull(impl.getOMMetadataManagerInstance()
              .getVolumeTable().getSkipCache("/vol" + i));
    }

    // test root response
    Response rootBasicRes = endpoint.getBasicInfo("/");
    NamespaceSummaryResponse rootBasicEntity =
            (NamespaceSummaryResponse) rootBasicRes.getEntity();
    Assert.assertSame(EntityType.ROOT, rootBasicEntity.getEntityType());
    // one additional dummy volume at creation
    Assert.assertEquals(13, rootBasicEntity.getCountStats().getNumVolume());
    Assert.assertEquals(12, rootBasicEntity.getCountStats().getNumBucket());
    Assert.assertEquals(12, rootBasicEntity.getCountStats().getNumTotalDir());
    Assert.assertEquals(12, rootBasicEntity.getCountStats().getNumTotalKey());
  }


  /**
   * Test case for verifying directory deletion and namespace summary updates.
   * Three cases are tested:
   * CASE-1: Creating a directory structure with files and verifying OM and
   *          Recon tables.
   * CASE-2: Deleting files from the directory and verifying updated NS summary.
   * CASE-3: Deleting the entire directory and confirming the NS summary.
   */
  @Test
  public void testNamespaceSummaryUpdatesForDirectoryDeletion()
      throws Exception {

    // CASE-1
    // Create a directory structure with 10 files in dir1
    addKeysToDirectory(1, 10, "/dir1");

    // Fetch the file table and directory table from Ozone Manager.
    OMMetadataManager ozoneMetadataManagerInstance =
        cluster.getOzoneManager().getMetadataManager();
    Table<String, OmKeyInfo> omFileTable =
        ozoneMetadataManagerInstance.getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> omDirTable =
        ozoneMetadataManagerInstance.getDirectoryTable();

    // Verify the entries in the Ozone Manager tables.
    assertOmTableRowCount(omFileTable, 10);
    assertOmTableRowCount(omDirTable, 1);

    // Sync data from Ozone Manager to Recon.
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();

    // Retrieve tables from Recon's OM-DB.
    ReconOMMetadataManager reconOmMetadataManagerInstance =
        (ReconOMMetadataManager) cluster.getReconServer()
            .getOzoneManagerServiceProvider().getOMMetadataManagerInstance();
    Table<String, OmKeyInfo> reconFileTable =
        reconOmMetadataManagerInstance.getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> reconDirTable =
        reconOmMetadataManagerInstance.getDirectoryTable();
    Table<String, OmKeyInfo> reconDeletedDirTable =
        reconOmMetadataManagerInstance.getDeletedDirTable();

    // Verify the entries in the Recon tables after sync.
    assertReconTableRowCount(reconFileTable, 10);
    assertReconTableRowCount(reconDirTable, 1);
    assertReconTableRowCount(reconDeletedDirTable, 0);

    // Retrieve the object ID of dir1 from directory table.
    Long directoryObjectId = null;
    try (
        TableIterator<?, ? extends Table.KeyValue<?, OmDirectoryInfo>> iterator
            = reconDirTable.iterator()) {
      if (iterator.hasNext()) {
        directoryObjectId = iterator.next().getValue().getObjectID();
      }
    }

    // Retrieve Namespace Summary for dir1 from Recon.
    ReconNamespaceSummaryManagerImpl namespaceSummaryManager =
        (ReconNamespaceSummaryManagerImpl) cluster.getReconServer()
            .getReconNamespaceSummaryManager();
    NSSummary summary =
        namespaceSummaryManager.getNSSummary(directoryObjectId);
    // Assert that the directory dir1 has 10 sub-files and size of 1000 bytes.
    Assert.assertEquals(10, summary.getNumOfFiles());
    Assert.assertEquals(1000, summary.getSizeOfFiles());

    // CASE-2
    // Delete 3 files from the directory dir1.
    deleteKeysFromDirectory(1, 3, "/dir1");
    impl.syncDataFromOM();
    // Fetch the updated namespace summary for the directory dir1.
    summary =
        namespaceSummaryManager.getNSSummary(directoryObjectId);
    // Assert that the directory dir1 now has 7 sub-files and size of 700 bytes.
    Assert.assertEquals(7, summary.getNumOfFiles());
    Assert.assertEquals(700, summary.getSizeOfFiles());

    // CASE-3
    // Delete the entire directory dir1.
    deleteDirectory("vol", "bucket", "/dir1");
    impl.syncDataFromOM();
    // Check the count of recon directory table and recon deletedDirectory table
    assertReconTableRowCount(reconDirTable, 0);
    assertReconTableRowCount(reconDeletedDirTable, 1);

    // Confirm even though the dir1 is deleted from OM, the namespace summary
    // for dir1 still exists.
    summary = namespaceSummaryManager.getNSSummary(directoryObjectId);
    // Assert that the deleted directory dir1 still has 7 files under them.
    Assert.assertEquals(7, summary.getNumOfFiles());
    Assert.assertEquals(700, summary.getSizeOfFiles());
  }


  /**
   * Helper function to add voli/bucketi/keyi to containeri to OM Metadata.
   * For test purpose each container will have only one key.
   */
  private void addKeys(int start, int end, String dirPrefix) throws Exception {
    for (int i = start; i < end; i++) {
      writeKeys("vol" + i, "bucket" + i, dirPrefix + i + "/key" + i);
    }
  }

  /**
   * This method is designed to create a series of keys with incremental indices
   * and associate them with a given directory. The keys are added to a
   * specified volume and bucket.
   */
  private void addKeysToDirectory(int startIndex, int endIndex, String dirName)
      throws Exception {
    store.createVolume("vol");
    OzoneVolume volume = store.getVolume("vol");
    volume.createBucket("bucket");
    for (int i = startIndex; i <= endIndex; i++) {
      writeTestData("vol", "bucket", dirName + "/key" + i);
    }
  }

  /**
   * This method is designed to delete a series of keys with incremental indices
   * and associate them with a given directory. The keys are deleted from a
   * specified volume and bucket.
   */
  private void deleteKeysFromDirectory(int startIndex, int endIndex,
                                       String dirName)
      throws Exception {
    for (int i = startIndex; i <= endIndex; i++) {
      deleteKey("vol", "bucket", dirName + "/key" + i);
    }
  }

  /**
   * Helper method to assert the row count of a given table in the OM metadata.
   * This method waits for a specific period of time for the row count of the
   * specified table to match the expected count.
   */
  private void assertOmTableRowCount(Table<String, ?> table, int count)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> assertOmTableRowCount(count, table), 1000,
        120000); // 2 minutes
  }

  private boolean assertOmTableRowCount(int expectedCount,
                                        Table<String, ?> table) {
    long count = 0L;
    try {
      count = cluster.getOzoneManager().getMetadataManager()
          .countRowsInTable(table);
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count, expectedCount);
    } catch (IOException ex) {
      fail("testDoubleBuffer failed with: " + ex);
    }
    return count == expectedCount;
  }

  /**
   * Helper method to assert the row count of a given table in the Recon
   * metadata. This method waits for a specific period of time for the row count
   * of the specified table to match the expected count.
   */
  private void assertReconTableRowCount(Table<String, ?> table, int count)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> assertReconTableRowCount(count, table), 1000,
        120000); // 2 minutes
  }

  private boolean assertReconTableRowCount(int expectedCount,
                                           Table<String, ?> table) {
    long count = 0L;
    try {
      count = cluster.getReconServer().getOzoneManagerServiceProvider()
          .getOMMetadataManagerInstance().countRowsInTable(table);
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count, expectedCount);
    } catch (IOException ex) {
      fail("testDoubleBuffer failed with: " + ex);
    }
    return count == expectedCount;
  }

  /**
   * Get the BucketLayout with the FileSystem Optimized configuration.
   */
  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

}
