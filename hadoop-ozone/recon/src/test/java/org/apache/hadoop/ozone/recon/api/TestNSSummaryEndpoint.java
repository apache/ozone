/*'
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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProviderWithFSO;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for NSSummary REST APIs.
 * We tested on a mini file system with the following setting:
 *                vol
 *             /       \
 *        bucket1      bucket2
 *        /    \         /    \
 *     file1    dir1    file4  file5
 *           /   \   \
 *        dir2  dir3  dir4
 *         /     \      \
 *       file2   file3  file6
 * This is a test for the Rest APIs only. We have tested NSSummaryTask before,
 * so there is no need to test process() on DB's updates
 */
public class TestNSSummaryEndpoint {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;
  private NSSummaryEndpoint nsSummaryEndpoint;

  private static final String TEST_PATH_UTILITY =
          "/vol1/buck1/a/b/c/d/e/file1.txt";
  private static final String PARENT_DIR = "vol1/buck1/a/b/c/d/e";
  private static final String[] TEST_NAMES =
          new String[]{"vol1", "buck1", "a", "b", "c", "d", "e", "file1.txt"};
  private static final String TEST_KEY_NAMES = "a/b/c/d/e/file1.txt";

  // Object names in FSO-enabled format
  private static final String VOL = "vol";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "dir1/dir2/file2";
  private static final String KEY_THREE = "dir1/dir3/file3";
  private static final String KEY_FOUR = "file4";
  private static final String KEY_FIVE = "file5";
  private static final String KEY_SIX = "dir1/dir4/file6";
  private static final String MULTI_BLOCK_KEY = "dir1/file7";
  private static final String MULTI_BLOCK_FILE = "file7";
  private static final String FILE_ONE = "file1";
  private static final String FILE_TWO = "file2";
  private static final String FILE_THREE = "file3";
  private static final String FILE_FOUR = "file4";
  private static final String FILE_FIVE = "file5";
  private static final String FILE_SIX = "file6";
  private static final String DIR_ONE = "dir1";
  private static final String DIR_TWO = "dir2";
  private static final String DIR_THREE = "dir3";
  private static final String DIR_FOUR = "dir4";

  // objects IDs
  private static final long VOL_OBJECT_ID = 0L;
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long DIR_ONE_OBJECT_ID = 4L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long KEY_FOUR_OBJECT_ID = 6L;
  private static final long DIR_TWO_OBJECT_ID = 7L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long KEY_FIVE_OBJECT_ID = 9L;
  private static final long KEY_SIX_OBJECT_ID = 10L;
  private static final long DIR_THREE_OBJECT_ID = 11L;
  private static final long DIR_FOUR_OBJECT_ID = 12L;
  private static final long MULTI_BLOCK_KEY_OBJECT_ID = 13L;

  // container IDs
  private static final long CONTAINER_ONE_ID = 1L;
  private static final long CONTAINER_TWO_ID = 2L;
  private static final long CONTAINER_THREE_ID = 3L;

  // replication factors
  private static final int THREE = 3;
  private static final int TWO = 2;
  private static final int FOUR = 4;

  // block lengths
  private static final long BLOCK_ONE_LENGTH = 1000L;
  private static final long BLOCK_TWO_LENGTH = 2000L;
  private static final long BLOCK_THREE_LENGTH = 3000L;

  // data size in bytes
  private static final long KEY_ONE_SIZE = 500L; // bin 0
  private static final long KEY_TWO_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_THREE_SIZE = 4 * OzoneConsts.KB + 1; // bin 3
  private static final long KEY_FOUR_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_FIVE_SIZE = 100L; // bin 0
  private static final long KEY_SIX_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long MULTI_BLOCK_KEY_SIZE_WITH_REPLICA
          = THREE * BLOCK_ONE_LENGTH
          + TWO * BLOCK_TWO_LENGTH
          + FOUR * BLOCK_THREE_LENGTH;

  // quota in bytes
  private static final long VOL_QUOTA = 2 * OzoneConsts.MB;
  private static final long BUCKET_ONE_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_TWO_QUOTA = OzoneConsts.MB;

  // mock client's path requests
  private static final String TEST_USER = "TestUser";
  private static final String VOL_PATH = "/vol";
  private static final String BUCKET_ONE_PATH = "/vol/bucket1";
  private static final String BUCKET_TWO_PATH = "/vol/bucket2";
  private static final String DIR_ONE_PATH = "/vol/bucket1/dir1";
  private static final String DIR_TWO_PATH = "/vol/bucket1/dir1/dir2";
  private static final String DIR_THREE_PATH = "/vol/bucket1/dir1/dir3";
  private static final String DIR_FOUR_PATH = "/vol/bucket1/dir1/dir4";
  private static final String KEY_PATH = "/vol/bucket2/file4";
  private static final String MULTI_BLOCK_KEY_PATH = "/vol/bucket1/dir1/file7";
  private static final String INVALID_PATH = "/vol/path/not/found";

  // some expected answers
  private static final long TOTAL_DATA_SIZE = KEY_ONE_SIZE + KEY_TWO_SIZE +
          KEY_THREE_SIZE + KEY_FOUR_SIZE + KEY_FIVE_SIZE + KEY_SIX_SIZE;

  private static final long BUCKET_ONE_DATA_SIZE = KEY_ONE_SIZE + KEY_TWO_SIZE +
          KEY_THREE_SIZE + KEY_SIX_SIZE;

  private static final long BUCKET_TWO_DATA_SIZE =
          KEY_FOUR_SIZE + KEY_FIVE_SIZE;

  private static final long DIR_ONE_DATA_SIZE = KEY_TWO_SIZE +
          KEY_THREE_SIZE + KEY_SIX_SIZE;

  @Before
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager(
            temporaryFolder.newFolder());
    ozoneManagerServiceProvider =
            getMockOzoneManagerServiceProviderWithFSO();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
            temporaryFolder.newFolder());

    ReconTestInjector reconTestInjector =
            new ReconTestInjector.Builder(temporaryFolder)
                    .withReconOm(reconOMMetadataManager)
                    .withOmServiceProvider(ozoneManagerServiceProvider)
                    .withReconSqlDb()
                    .withContainerDB()
                    .addBinding(OzoneStorageContainerManager.class,
                            getMockReconSCM())
                    .addBinding(StorageContainerServiceProvider.class,
                            mock(StorageContainerServiceProviderImpl.class))
                    .addBinding(NSSummaryEndpoint.class)
                    .build();
    reconNamespaceSummaryManager =
            reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    nsSummaryEndpoint = reconTestInjector.getInstance(NSSummaryEndpoint.class);

    // populate OM DB and reprocess into Recon RocksDB
    populateOMDB();
    NSSummaryTask nsSummaryTask =
            new NSSummaryTask(reconNamespaceSummaryManager);
    nsSummaryTask.reprocess(reconOMMetadataManager);
  }

  @Test
  public void testUtility() {
    String[] names = NSSummaryEndpoint.parseRequestPath(TEST_PATH_UTILITY);
    Assert.assertArrayEquals(TEST_NAMES, names);
    String keyName = NSSummaryEndpoint.getKeyName(names);
    Assert.assertEquals(TEST_KEY_NAMES, keyName);
    String subpath = NSSummaryEndpoint.buildSubpath(PARENT_DIR, "file1.txt");
    Assert.assertEquals(TEST_PATH_UTILITY, subpath);
  }

  @Test
  public void testBasic() throws Exception {
    // Test volume basics
    Response volResponse = nsSummaryEndpoint.getBasicInfo(VOL_PATH);
    NamespaceSummaryResponse volResponseObj =
            (NamespaceSummaryResponse) volResponse.getEntity();
    Assert.assertEquals(EntityType.VOLUME, volResponseObj.getEntityType());
    Assert.assertEquals(2, volResponseObj.getNumBucket());
    Assert.assertEquals(4, volResponseObj.getNumTotalDir());
    Assert.assertEquals(6, volResponseObj.getNumTotalKey());

    // Test bucket 1's basics
    Response bucketOneResponse =
            nsSummaryEndpoint.getBasicInfo(BUCKET_ONE_PATH);
    NamespaceSummaryResponse bucketOneObj =
            (NamespaceSummaryResponse) bucketOneResponse.getEntity();
    Assert.assertEquals(EntityType.BUCKET, bucketOneObj.getEntityType());
    Assert.assertEquals(4, bucketOneObj.getNumTotalDir());
    Assert.assertEquals(4, bucketOneObj.getNumTotalKey());

    // Test bucket 2's basics
    Response bucketTwoResponse =
            nsSummaryEndpoint.getBasicInfo(BUCKET_TWO_PATH);
    NamespaceSummaryResponse bucketTwoObj =
            (NamespaceSummaryResponse) bucketTwoResponse.getEntity();
    Assert.assertEquals(EntityType.BUCKET, bucketTwoObj.getEntityType());
    Assert.assertEquals(0, bucketTwoObj.getNumTotalDir());
    Assert.assertEquals(2, bucketTwoObj.getNumTotalKey());

    // Test intermediate directory basics
    Response dirOneResponse = nsSummaryEndpoint.getBasicInfo(DIR_ONE_PATH);
    NamespaceSummaryResponse dirOneObj =
            (NamespaceSummaryResponse) dirOneResponse.getEntity();
    Assert.assertEquals(EntityType.DIRECTORY, dirOneObj.getEntityType());
    Assert.assertEquals(3, dirOneObj.getNumTotalDir());
    Assert.assertEquals(3, dirOneObj.getNumTotalKey());

    // Test invalid path
    Response invalidResponse = nsSummaryEndpoint.getBasicInfo(INVALID_PATH);
    NamespaceSummaryResponse invalidObj =
            (NamespaceSummaryResponse) invalidResponse.getEntity();
    Assert.assertEquals(ResponseStatus.PATH_NOT_FOUND,
            invalidObj.getStatus());

    // Test key
    Response keyResponse = nsSummaryEndpoint.getBasicInfo(KEY_PATH);
    NamespaceSummaryResponse keyResObj =
            (NamespaceSummaryResponse) keyResponse.getEntity();
    Assert.assertEquals(EntityType.KEY, keyResObj.getEntityType());
  }

  @Test
  public void testDiskUsage() throws Exception {
    // volume level DU
    Response volResponse = nsSummaryEndpoint.getDiskUsage(VOL_PATH,
            false, false);
    DUResponse duVolRes = (DUResponse) volResponse.getEntity();
    Assert.assertEquals(2, duVolRes.getCount());
    List<DUResponse.DiskUsage> duData = duVolRes.getDuData();
    // sort based on subpath
    Collections.sort(duData,
            Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    DUResponse.DiskUsage duBucket1 = duData.get(0);
    DUResponse.DiskUsage duBucket2 = duData.get(1);
    Assert.assertEquals(BUCKET_ONE_PATH, duBucket1.getSubpath());
    Assert.assertEquals(BUCKET_TWO_PATH, duBucket2.getSubpath());
    Assert.assertEquals(BUCKET_ONE_DATA_SIZE, duBucket1.getSize());
    Assert.assertEquals(BUCKET_TWO_DATA_SIZE, duBucket2.getSize());

    // bucket level DU
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_ONE_PATH,
            false, false);
    DUResponse duBucketResponse = (DUResponse) bucketResponse.getEntity();
    Assert.assertEquals(1, duBucketResponse.getCount());
    DUResponse.DiskUsage duDir1 = duBucketResponse.getDuData().get(0);
    Assert.assertEquals(DIR_ONE_PATH, duDir1.getSubpath());
    Assert.assertEquals(DIR_ONE_DATA_SIZE, duDir1.getSize());

    // dir level DU
    Response dirResponse = nsSummaryEndpoint.getDiskUsage(DIR_ONE_PATH,
            false, false);
    DUResponse duDirReponse = (DUResponse) dirResponse.getEntity();
    Assert.assertEquals(3, duDirReponse.getCount());
    List<DUResponse.DiskUsage> duSubDir = duDirReponse.getDuData();
    Collections.sort(duSubDir,
            Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    DUResponse.DiskUsage duDir2 = duSubDir.get(0);
    DUResponse.DiskUsage duDir3 = duSubDir.get(1);
    DUResponse.DiskUsage duDir4 = duSubDir.get(2);
    Assert.assertEquals(DIR_TWO_PATH, duDir2.getSubpath());
    Assert.assertEquals(KEY_TWO_SIZE, duDir2.getSize());

    Assert.assertEquals(DIR_THREE_PATH, duDir3.getSubpath());
    Assert.assertEquals(KEY_THREE_SIZE, duDir3.getSize());

    Assert.assertEquals(DIR_FOUR_PATH, duDir4.getSubpath());
    Assert.assertEquals(KEY_SIX_SIZE, duDir4.getSize());

    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_PATH,
            false, false);
    DUResponse keyObj = (DUResponse) keyResponse.getEntity();
    Assert.assertEquals(0, keyObj.getCount());
    Assert.assertEquals(KEY_FOUR_SIZE, keyObj.getSize());

    // invalid path check
    Response invalidResponse = nsSummaryEndpoint.getDiskUsage(INVALID_PATH,
            false, false);
    DUResponse invalidObj = (DUResponse) invalidResponse.getEntity();
    Assert.assertEquals(ResponseStatus.PATH_NOT_FOUND,
            invalidObj.getStatus());
  }

  @Test
  public void testDiskUsageWithReplication() throws Exception {
    setUpMultiBlockKey();
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(MULTI_BLOCK_KEY_PATH,
            false, true);
    DUResponse replicaDUResponse = (DUResponse) keyResponse.getEntity();
    Assert.assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    Assert.assertEquals(MULTI_BLOCK_KEY_SIZE_WITH_REPLICA,
            replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testQuotaUsage() throws Exception {
    // volume level quota usage
    Response volResponse = nsSummaryEndpoint.getQuotaUsage(VOL_PATH);
    QuotaUsageResponse quVolRes = (QuotaUsageResponse) volResponse.getEntity();
    Assert.assertEquals(VOL_QUOTA, quVolRes.getQuota());
    Assert.assertEquals(TOTAL_DATA_SIZE, quVolRes.getQuotaUsed());

    // bucket level quota usage
    Response bucketRes = nsSummaryEndpoint.getQuotaUsage(BUCKET_ONE_PATH);
    QuotaUsageResponse quBucketRes = (QuotaUsageResponse) bucketRes.getEntity();
    Assert.assertEquals(BUCKET_ONE_QUOTA, quBucketRes.getQuota());
    Assert.assertEquals(BUCKET_ONE_DATA_SIZE, quBucketRes.getQuotaUsed());

    Response bucketRes2 = nsSummaryEndpoint.getQuotaUsage(BUCKET_TWO_PATH);
    QuotaUsageResponse quBucketRes2 =
            (QuotaUsageResponse) bucketRes2.getEntity();
    Assert.assertEquals(BUCKET_TWO_QUOTA, quBucketRes2.getQuota());
    Assert.assertEquals(BUCKET_TWO_DATA_SIZE, quBucketRes2.getQuotaUsed());

    // other level not applicable
    Response naResponse1 = nsSummaryEndpoint.getQuotaUsage(DIR_ONE_PATH);
    QuotaUsageResponse quotaUsageResponse1 =
            (QuotaUsageResponse) naResponse1.getEntity();
    Assert.assertEquals(ResponseStatus.TYPE_NOT_APPLICABLE,
            quotaUsageResponse1.getResponseCode());

    Response naResponse2 = nsSummaryEndpoint.getQuotaUsage(KEY_PATH);
    QuotaUsageResponse quotaUsageResponse2 =
            (QuotaUsageResponse) naResponse2.getEntity();
    Assert.assertEquals(ResponseStatus.TYPE_NOT_APPLICABLE,
            quotaUsageResponse2.getResponseCode());

    // invalid path request
    Response invalidRes = nsSummaryEndpoint.getQuotaUsage(INVALID_PATH);
    QuotaUsageResponse invalidResObj =
            (QuotaUsageResponse) invalidRes.getEntity();
    Assert.assertEquals(ResponseStatus.PATH_NOT_FOUND,
            invalidResObj.getResponseCode());
  }

  /**
   * Bin 0: 2 -> file1 and file5.
   * Bin 1: 1 -> file2.
   * Bin 2: 2 -> file4 and file6.
   * Bin 3: 1 -> file3.
   * @throws Exception
   */
  @Test
  public void testFileSizeDist() throws Exception {
    Response volRes = nsSummaryEndpoint.getFileSizeDistribution(VOL_PATH);
    FileSizeDistributionResponse volFileSizeDistResObj =
            (FileSizeDistributionResponse) volRes.getEntity();
    // If the volume has the correct file size distribution,
    // other lower level should be correct as well, given all
    // other previous tests have passed.
    int[] volFileSizeDist = volFileSizeDistResObj.getFileSizeDist();
    for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
      if (i == 0 || i == 2) {
        Assert.assertEquals(2, volFileSizeDist[i]);
      } else if (i == 1 || i == 3) {
        Assert.assertEquals(1, volFileSizeDist[i]);
      } else {
        Assert.assertEquals(0, volFileSizeDist[i]);
      }
    }
  }

  /**
   * Write directories and keys info into OM DB.
   * @throws Exception
   */
  private void populateOMDB() throws Exception {
    // write all 4 directories
    writeDirToOm(reconOMMetadataManager, DIR_ONE_OBJECT_ID,
            BUCKET_ONE_OBJECT_ID, DIR_ONE);
    writeDirToOm(reconOMMetadataManager, DIR_TWO_OBJECT_ID,
            DIR_ONE_OBJECT_ID, DIR_TWO);
    writeDirToOm(reconOMMetadataManager, DIR_THREE_OBJECT_ID,
            DIR_ONE_OBJECT_ID, DIR_THREE);
    writeDirToOm(reconOMMetadataManager, DIR_FOUR_OBJECT_ID,
            DIR_ONE_OBJECT_ID, DIR_FOUR);

    // write all 6 keys
    writeKeyToOm(reconOMMetadataManager,
            KEY_ONE,
            BUCKET_ONE,
            VOL,
            FILE_ONE,
            KEY_ONE_OBJECT_ID,
            BUCKET_ONE_OBJECT_ID,
            KEY_ONE_SIZE);
    writeKeyToOm(reconOMMetadataManager,
            KEY_TWO,
            BUCKET_ONE,
            VOL,
            FILE_TWO,
            KEY_TWO_OBJECT_ID,
            DIR_TWO_OBJECT_ID,
            KEY_TWO_SIZE);
    writeKeyToOm(reconOMMetadataManager,
            KEY_THREE,
            BUCKET_ONE,
            VOL,
            FILE_THREE,
            KEY_THREE_OBJECT_ID,
            DIR_THREE_OBJECT_ID,
            KEY_THREE_SIZE);
    writeKeyToOm(reconOMMetadataManager,
            KEY_FOUR,
            BUCKET_TWO,
            VOL,
            FILE_FOUR,
            KEY_FOUR_OBJECT_ID,
            BUCKET_TWO_OBJECT_ID,
            KEY_FOUR_SIZE);
    writeKeyToOm(reconOMMetadataManager,
            KEY_FIVE,
            BUCKET_TWO,
            VOL,
            FILE_FIVE,
            KEY_FIVE_OBJECT_ID,
            BUCKET_TWO_OBJECT_ID,
            KEY_FIVE_SIZE);
    writeKeyToOm(reconOMMetadataManager,
            KEY_SIX,
            BUCKET_ONE,
            VOL,
            FILE_SIX,
            KEY_SIX_OBJECT_ID,
            DIR_FOUR_OBJECT_ID,
            KEY_SIX_SIZE);
  }


  /**
   * Create a new OM Metadata manager instance with one user, one vol, and two
   * buckets.
   * @throws IOException ioEx
   */
  private static OMMetadataManager initializeNewOmMetadataManager(
          File omDbDir)
          throws IOException {
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS,
            omDbDir.getAbsolutePath());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
            omConfiguration);

    String volumeKey = omMetadataManager.getVolumeKey(VOL);
    OmVolumeArgs args =
            OmVolumeArgs.newBuilder()
                    .setObjectID(VOL_OBJECT_ID)
                    .setVolume(VOL)
                    .setAdminName(TEST_USER)
                    .setOwnerName(TEST_USER)
                    .setQuotaInBytes(VOL_QUOTA)
                    .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(VOL)
            .setBucketName(BUCKET_ONE)
            .setObjectID(BUCKET_ONE_OBJECT_ID)
            .setQuotaInBytes(BUCKET_ONE_QUOTA)
            .build();

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
            .setVolumeName(VOL)
            .setBucketName(BUCKET_TWO)
            .setObjectID(BUCKET_TWO_OBJECT_ID)
            .setQuotaInBytes(BUCKET_TWO_QUOTA)
            .build();

    String bucketKey = omMetadataManager.getBucketKey(
            bucketInfo.getVolumeName(), bucketInfo.getBucketName());
    String bucketKey2 = omMetadataManager.getBucketKey(
            bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());

    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo);
    omMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);

    return omMetadataManager;
  }

  private void setUpMultiBlockKey() throws IOException {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    BlockID block1 = new BlockID(CONTAINER_ONE_ID, 0L);
    BlockID block2 = new BlockID(CONTAINER_TWO_ID, 0L);
    BlockID block3 = new BlockID(CONTAINER_THREE_ID, 0L);

    OmKeyLocationInfo location1 = new OmKeyLocationInfo.Builder()
            .setBlockID(block1)
            .setLength(BLOCK_ONE_LENGTH)
            .build();
    OmKeyLocationInfo location2 = new OmKeyLocationInfo.Builder()
            .setBlockID(block2)
            .setLength(BLOCK_TWO_LENGTH)
            .build();
    OmKeyLocationInfo location3 = new OmKeyLocationInfo.Builder()
            .setBlockID(block3)
            .setLength(BLOCK_THREE_LENGTH)
            .build();
    locationInfoList.add(location1);
    locationInfoList.add(location2);
    locationInfoList.add(location3);

    OmKeyLocationInfoGroup locationInfoGroup =
            new OmKeyLocationInfoGroup(0L, locationInfoList);

    // add the multi-block key to Recon's OM
    writeKeyToOm(reconOMMetadataManager,
            DIR_ONE_OBJECT_ID,
            MULTI_BLOCK_KEY_OBJECT_ID,
            VOL, BUCKET_ONE,
            MULTI_BLOCK_KEY,
            MULTI_BLOCK_FILE,
            Collections.singletonList(locationInfoGroup));
  }

  /**
   * Generate a set of mock container replica with a size of
   * replication factor for container.
   * @param replicationFactor number of replica
   * @param containerID the container replicated based upon
   * @return a set of container replica for testing
   */
  private static Set<ContainerReplica> generateMockContainerReplicas(
          int replicationFactor, ContainerID containerID) {
    Set<ContainerReplica> result = new HashSet<>();
    for (int i = 0; i < replicationFactor; ++i) {
      DatanodeDetails randomDatanode = randomDatanodeDetails();
      ContainerReplica replica = new ContainerReplica.ContainerReplicaBuilder()
              .setContainerID(containerID)
              .setContainerState(State.OPEN)
              .setDatanodeDetails(randomDatanode)
              .build();
      result.add(replica);
    }
    return result;
  }

  private static ReconStorageContainerManagerFacade getMockReconSCM()
          throws ContainerNotFoundException {
    ReconStorageContainerManagerFacade reconSCM =
            mock(ReconStorageContainerManagerFacade.class);
    ContainerManager containerManager = mock(ContainerManager.class);

    // Container 1 is 3-way replicated
    ContainerID containerID1 = new ContainerID(CONTAINER_ONE_ID);
    Set<ContainerReplica> containerReplicas1 = generateMockContainerReplicas(
            THREE, containerID1);
    when(containerManager.getContainerReplicas(containerID1))
            .thenReturn(containerReplicas1);

    // Container 2 is under replicated with 2 replica
    ContainerID containerID2 = new ContainerID(CONTAINER_TWO_ID);
    Set<ContainerReplica> containerReplicas2 = generateMockContainerReplicas(
            TWO, containerID2);
    when(containerManager.getContainerReplicas(containerID2))
            .thenReturn(containerReplicas2);

    // Container 3 is over replicated with 4 replica
    ContainerID containerID3 = new ContainerID(CONTAINER_THREE_ID);
    Set<ContainerReplica> containerReplicas3 = generateMockContainerReplicas(
            FOUR, containerID3);
    when(containerManager.getContainerReplicas(containerID3))
            .thenReturn(containerReplicas3);

    when(reconSCM.getContainerManager()).thenReturn(containerManager);
    return reconSCM;
  }
}
