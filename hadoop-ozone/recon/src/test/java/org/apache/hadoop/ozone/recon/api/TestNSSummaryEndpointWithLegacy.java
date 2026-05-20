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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.helpers.QuotaUtil.getReplicatedSize;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.setConfiguration;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithLegacy;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for NSSummary REST APIs with Legacy.
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
 *  ----------------------------------------
 *                  vol2
 *              /         \
 *      bucket3          bucket4
 *      /      \           /
 *   file8     dir5      file11
 *            /    \
 *        file9    file10
 *  ----------------------------------------
 *                    vol3
 *                     |
 *                 bucket5
 *                 /      \
 *             file12     dir6
 *                     /    \
 *                 file13    dir7
 *                          /
 *                          file14
 * This is a test for the Rest APIs only. We have tested NSSummaryTask before,
 * so there is no need to test process() on DB's updates
 */
public class TestNSSummaryEndpointWithLegacy extends NSSummaryTests {
  @TempDir
  private Path temporaryFolder;

  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private NSSummaryEndpoint nsSummaryEndpoint;

  private static final String TEST_PATH_UTILITY =
      "/vol1/buck1/a/b/c/d/e/file1.txt";
  private static final String PARENT_DIR = "vol1/buck1/a/b/c/d/e";
  private static final String[] TEST_NAMES =
      new String[]{"vol1", "buck1", "a", "b", "c", "d", "e", "file1.txt"};
  private static final String TEST_KEY_NAMES = "a/b/c/d/e/file1.txt";

  // Object names
  private static final String VOL = "vol";
  private static final String VOL_TWO = "vol2";
  private static final String VOL_THREE = "vol3";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String BUCKET_THREE = "bucket3";
  private static final String BUCKET_FOUR = "bucket4";
  private static final String BUCKET_FIVE = "bucket5";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "dir1/dir2/file2";
  private static final String KEY_THREE = "dir1/dir3/file3";
  private static final String KEY_FOUR = "file4";
  private static final String KEY_FIVE = "file5";
  private static final String KEY_SIX = "dir1/dir4/file6";
  private static final String KEY_SEVEN = "dir1/file7";
  private static final String KEY_EIGHT = "file8";
  private static final String KEY_NINE = "dir5/file9";
  private static final String KEY_TEN = "dir5/file10";
  private static final String KEY_ELEVEN = "file11";
  private static final String MULTI_BLOCK_KEY = "dir1/file7";
  private static final String MULTI_BLOCK_FILE = "file7";
  private static final String KEY_TWELVE = "file12";
  private static final String KEY_THIRTEEN = "dir6/file13";
  private static final String KEY_FOURTEEN = "dir6/dir7/file14";

  private static final String FILE_ONE = "file1";
  private static final String FILE_TWO = "file2";
  private static final String FILE_THREE = "file3";
  private static final String FILE_FOUR = "file4";
  private static final String FILE_FIVE = "file5";
  private static final String FILE_SIX = "file6";
  private static final String FILE_SEVEN = "file7";
  private static final String FILE_EIGHT = "file8";
  private static final String FILE_NINE = "file9";
  private static final String FILE_TEN = "file10";
  private static final String FILE_ELEVEN = "file11";
  private static final String FILE_TWELVE = "file12";
  private static final String FILE_THIRTEEN = "file13";
  private static final String FILE_FOURTEEN = "file14";

  private static final String DIR_ONE = "dir1";
  private static final String DIR_TWO = "dir2";
  private static final String DIR_THREE = "dir3";
  private static final String DIR_FOUR = "dir4";
  private static final String DIR_FIVE = "dir5";
  private static final String DIR_SIX = "dir6";
  private static final String DIR_SEVEN = "dir7";
  // objects IDs
  private static final long PARENT_OBJECT_ID_ZERO = 0L;
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
  private static final long KEY_SEVEN_OBJECT_ID = 13L;
  private static final long VOL_TWO_OBJECT_ID = 14L;
  private static final long BUCKET_THREE_OBJECT_ID = 15L;
  private static final long BUCKET_FOUR_OBJECT_ID = 16L;
  private static final long KEY_EIGHT_OBJECT_ID = 17L;
  private static final long DIR_FIVE_OBJECT_ID = 18L;
  private static final long KEY_NINE_OBJECT_ID = 19L;
  private static final long KEY_TEN_OBJECT_ID = 20L;
  private static final long KEY_ELEVEN_OBJECT_ID = 21L;
  private static final long VOL_THREE_OBJECT_ID = 22L;
  private static final long DIR_SIX_OBJECT_ID = 23L;
  private static final long DIR_SEVEN_OBJECT_ID = 24L;
  private static final long FILE_TWELVE_OBJECT_ID = 25L;
  private static final long FILE_THIRTEEN_OBJECT_ID = 26L;
  private static final long FILE_FOURTEEN_OBJECT_ID = 27L;
  private static final long BUCKET_FIVE_OBJECT_ID = 28L;

  // container IDs
  private static final long CONTAINER_ONE_ID = 1L;
  private static final long CONTAINER_TWO_ID = 2L;
  private static final long CONTAINER_THREE_ID = 3L;
  private static final long CONTAINER_FOUR_ID = 4L;
  private static final long CONTAINER_FIVE_ID = 5L;
  private static final long CONTAINER_SIX_ID = 6L;

  // replication factors
  private static final int CONTAINER_ONE_REPLICA_COUNT  = 3;
  private static final int CONTAINER_TWO_REPLICA_COUNT  = 2;
  private static final int CONTAINER_THREE_REPLICA_COUNT  = 4;
  private static final int CONTAINER_FOUR_REPLICA_COUNT  = 5;
  private static final int CONTAINER_FIVE_REPLICA_COUNT  = 2;
  private static final int CONTAINER_SIX_REPLICA_COUNT  = 3;

  // block lengths
  private static final long BLOCK_ONE_LENGTH = 1000L;
  private static final long BLOCK_TWO_LENGTH = 2000L;
  private static final long BLOCK_THREE_LENGTH = 3000L;
  private static final long BLOCK_FOUR_LENGTH = 4000L;
  private static final long BLOCK_FIVE_LENGTH = 5000L;
  private static final long BLOCK_SIX_LENGTH = 6000L;

  // data size in bytes
  private static final long KEY_ONE_SIZE = 500L; // bin 0
  private static final long KEY_TWO_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_THREE_SIZE = 4 * OzoneConsts.KB + 1; // bin 3
  private static final long KEY_FOUR_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_FIVE_SIZE = 100L; // bin 0
  private static final long KEY_SIX_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_SEVEN_SIZE = 4 * OzoneConsts.KB + 1;
  private static final long KEY_EIGHT_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_NINE_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_TEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_ELEVEN_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_TWELVE_SIZE = OzoneConsts.KB;
  private static final long KEY_THIRTEEN_SIZE = OzoneConsts.KB;
  private static final long KEY_FOURTEEN_SIZE = OzoneConsts.KB;

  private static final long FILE1_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_ONE_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE2_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_TWO_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE3_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_THREE_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE4_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_FOUR_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE5_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_FIVE_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE6_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_SIX_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE7_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_SEVEN_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE8_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_EIGHT_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE9_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_NINE_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE10_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_TEN_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE11_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_ELEVEN_SIZE,
              StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE12_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_TWELVE_SIZE, RatisReplicationConfig.getInstance(THREE));
  private static final long FILE13_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_THIRTEEN_SIZE, RatisReplicationConfig.getInstance(THREE));
  private static final long FILE14_SIZE_WITH_REPLICA =
      getReplicatedSize(KEY_FOURTEEN_SIZE, RatisReplicationConfig.getInstance(THREE));

  private static final long MULTI_BLOCK_KEY_SIZE_WITH_REPLICA
          = FILE7_SIZE_WITH_REPLICA;
  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_ROOT
      = FILE1_SIZE_WITH_REPLICA
      + FILE2_SIZE_WITH_REPLICA
      + FILE3_SIZE_WITH_REPLICA
      + FILE4_SIZE_WITH_REPLICA
      + FILE5_SIZE_WITH_REPLICA
      + FILE6_SIZE_WITH_REPLICA
      + FILE7_SIZE_WITH_REPLICA
      + FILE8_SIZE_WITH_REPLICA
      + FILE9_SIZE_WITH_REPLICA
      + FILE10_SIZE_WITH_REPLICA
      + FILE11_SIZE_WITH_REPLICA
      + FILE12_SIZE_WITH_REPLICA
      + FILE13_SIZE_WITH_REPLICA
      + FILE14_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_VOL
      = FILE1_SIZE_WITH_REPLICA
      + FILE2_SIZE_WITH_REPLICA
      + FILE3_SIZE_WITH_REPLICA
      + FILE4_SIZE_WITH_REPLICA
      + FILE5_SIZE_WITH_REPLICA
      + FILE6_SIZE_WITH_REPLICA
      + FILE7_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_BUCKET1
      = FILE1_SIZE_WITH_REPLICA
      + FILE2_SIZE_WITH_REPLICA
      + FILE3_SIZE_WITH_REPLICA
      + FILE6_SIZE_WITH_REPLICA
      + FILE7_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_DIR1
      = FILE2_SIZE_WITH_REPLICA
      + FILE3_SIZE_WITH_REPLICA
      + FILE6_SIZE_WITH_REPLICA
      + FILE7_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_DIR2
      = FILE2_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_KEY
      = FILE4_SIZE_WITH_REPLICA;

  // quota in bytes
  private static final long ROOT_QUOTA = 2 * (2 * OzoneConsts.MB);
  private static final long VOL_QUOTA = 2 * OzoneConsts.MB;
  private static final long VOL_TWO_QUOTA = 2 * OzoneConsts.MB;
  private static final long VOL_THREE_QUOTA = 2 * OzoneConsts.MB;
  private static final long BUCKET_ONE_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_TWO_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_THREE_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_FOUR_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_FIVE_QUOTA = OzoneConsts.MB;

  // mock client's path requests
  private static final String TEST_USER = "TestUser";
  private static final String ROOT_PATH = "/";
  private static final String VOL_PATH = "/vol";
  private static final String VOL_TWO_PATH = "/vol2";
  private static final String BUCKET_ONE_PATH = "/vol/bucket1";
  private static final String BUCKET_TWO_PATH = "/vol/bucket2";
  private static final String DIR_ONE_PATH = "/vol/bucket1/dir1";
  private static final String DIR_TWO_PATH = "/vol/bucket1/dir1/dir2";
  private static final String DIR_THREE_PATH = "/vol/bucket1/dir1/dir3";
  private static final String DIR_FOUR_PATH = "/vol/bucket1/dir1/dir4";
  private static final String KEY_PATH = "/vol/bucket2/file4";
  private static final String MULTI_BLOCK_KEY_PATH = "/vol/bucket1/dir1/file7";
  private static final String INVALID_PATH = "/vol/path/not/found";
  private static final String VOL_THREE_PATH = "/vol3";

  // some expected answers
  private static final long ROOT_DATA_SIZE = KEY_ONE_SIZE + KEY_TWO_SIZE +
      KEY_THREE_SIZE + KEY_FOUR_SIZE + KEY_FIVE_SIZE + KEY_SIX_SIZE + KEY_SEVEN_SIZE +
      KEY_EIGHT_SIZE + KEY_NINE_SIZE + KEY_TEN_SIZE + KEY_ELEVEN_SIZE +
      FILE12_SIZE_WITH_REPLICA + FILE13_SIZE_WITH_REPLICA + FILE14_SIZE_WITH_REPLICA;
  private static final long VOL_DATA_SIZE = KEY_ONE_SIZE + KEY_TWO_SIZE +
      KEY_THREE_SIZE + KEY_FOUR_SIZE + KEY_FIVE_SIZE + KEY_SIX_SIZE + KEY_SEVEN_SIZE;

  private static final long VOL_TWO_DATA_SIZE =
      KEY_EIGHT_SIZE + KEY_NINE_SIZE + KEY_TEN_SIZE + KEY_ELEVEN_SIZE;

  private static final long BUCKET_ONE_DATA_SIZE = KEY_ONE_SIZE + KEY_TWO_SIZE +
      KEY_THREE_SIZE + KEY_SIX_SIZE + KEY_SEVEN_SIZE;

  private static final long BUCKET_TWO_DATA_SIZE =
      KEY_FOUR_SIZE + KEY_FIVE_SIZE;

  private static final long DIR_ONE_DATA_SIZE = KEY_TWO_SIZE +
      KEY_THREE_SIZE + KEY_SIX_SIZE + KEY_SEVEN_SIZE;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OMMetadataManager omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve(
            "JunitOmDBDir")).toFile(), conf);
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        getMockOzoneManagerServiceProvider();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(temporaryFolder.resolve(
            "omMetadatDir")).toFile());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
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
    this.reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    nsSummaryEndpoint = reconTestInjector.getInstance(NSSummaryEndpoint.class);

    // populate OM DB and reprocess into Recon RocksDB
    populateOMDB();
    populateVolumeThree();
    setUpMultiBlockReplicatedKeys();
    NSSummaryTaskWithLegacy nsSummaryTaskWithLegacy = 
        new NSSummaryTaskWithLegacy(reconNamespaceSummaryManager, 
                                    reconOMMetadataManager, conf, 10);
    nsSummaryTaskWithLegacy.reprocessWithLegacy(reconOMMetadataManager);
  }

  @Test
  public void testUtility() {
    String[] names = EntityHandler.parseRequestPath(TEST_PATH_UTILITY);
    assertArrayEquals(TEST_NAMES, names);
    String keyName = BucketHandler.getKeyName(names);
    assertEquals(TEST_KEY_NAMES, keyName);
    String subpath = BucketHandler.buildSubpath(PARENT_DIR, "file1.txt");
    assertEquals(TEST_PATH_UTILITY, subpath);
  }

  @Test
  public void testGetBasicInfoRoot() throws Exception {
    testNSSummaryBasicInfoRoot(
        nsSummaryEndpoint, reconOMMetadataManager);
  }

  @Test
  public void testGetBasicInfoVol() throws Exception {
    // Test volume basics
    testNSSummaryBasicInfoVolume(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoBucketOne() throws Exception {
    // Test bucket 1's basics
    testNSSummaryBasicInfoBucketOne(
        BucketLayout.LEGACY,
        nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoBucketTwo() throws Exception {
    // Test bucket 2's basics
    testNSSummaryBasicInfoBucketTwo(
        BucketLayout.LEGACY,
        nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoDir() throws Exception {
    // Test intermediate directory basics
    testNSSummaryBasicInfoDir(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoNoPath() throws Exception {
    // Test invalid path
    testNSSummaryBasicInfoNoPath(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoKey() throws Exception {
    // Test key
    testNSSummaryBasicInfoKey(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageRoot() throws Exception {
    // root level DU
    Response rootResponse = nsSummaryEndpoint.getDiskUsage(ROOT_PATH,
        false, false, false);
    DUResponse duRootRes = (DUResponse) rootResponse.getEntity();
    assertEquals(3, duRootRes.getCount());
    List<DUResponse.DiskUsage> duRootData = duRootRes.getDuData();
    // sort based on subpath
    Collections.sort(duRootData,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    DUResponse.DiskUsage duVol1 = duRootData.get(0);
    DUResponse.DiskUsage duVol2 = duRootData.get(1);
    assertEquals(VOL_PATH, duVol1.getSubpath());
    assertEquals(VOL_TWO_PATH, duVol2.getSubpath());
    assertEquals(VOL_DATA_SIZE, duVol1.getSize());
    assertEquals(VOL_TWO_DATA_SIZE, duVol2.getSize());
  }

  @Test
  public void testDiskUsageVolume() throws Exception {
    // volume level DU
    Response volResponse = nsSummaryEndpoint.getDiskUsage(VOL_PATH,
        false, false, false);
    DUResponse duVolRes = (DUResponse) volResponse.getEntity();
    assertEquals(2, duVolRes.getCount());
    List<DUResponse.DiskUsage> duData = duVolRes.getDuData();
    // sort based on subpath
    Collections.sort(duData,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    DUResponse.DiskUsage duBucket1 = duData.get(0);
    DUResponse.DiskUsage duBucket2 = duData.get(1);
    assertEquals(BUCKET_ONE_PATH, duBucket1.getSubpath());
    assertEquals(BUCKET_TWO_PATH, duBucket2.getSubpath());
    assertEquals(BUCKET_ONE_DATA_SIZE, duBucket1.getSize());
    assertEquals(BUCKET_TWO_DATA_SIZE, duBucket2.getSize());
  }

  @Test
  public void testDiskUsageBucket() throws Exception {
    // bucket level DU
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_ONE_PATH,
        false, false, false);
    DUResponse duBucketResponse = (DUResponse) bucketResponse.getEntity();
    assertEquals(1, duBucketResponse.getCount());
    DUResponse.DiskUsage duDir1 = duBucketResponse.getDuData().get(0);
    assertEquals(DIR_ONE_PATH, duDir1.getSubpath());
    assertEquals(DIR_ONE_DATA_SIZE, duDir1.getSize());
  }

  @Test
  public void testDiskUsageDir() throws Exception {
    // dir level DU
    Response dirResponse = nsSummaryEndpoint.getDiskUsage(DIR_ONE_PATH,
        false, false, false);
    DUResponse duDirReponse = (DUResponse) dirResponse.getEntity();
    assertEquals(3, duDirReponse.getCount());
    List<DUResponse.DiskUsage> duSubDir = duDirReponse.getDuData();
    Collections.sort(duSubDir,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    DUResponse.DiskUsage duDir2 = duSubDir.get(0);
    DUResponse.DiskUsage duDir3 = duSubDir.get(1);
    DUResponse.DiskUsage duDir4 = duSubDir.get(2);
    assertEquals(DIR_TWO_PATH, duDir2.getSubpath());
    assertEquals(KEY_TWO_SIZE, duDir2.getSize());

    assertEquals(DIR_THREE_PATH, duDir3.getSubpath());
    assertEquals(KEY_THREE_SIZE, duDir3.getSize());

    assertEquals(DIR_FOUR_PATH, duDir4.getSubpath());
    assertEquals(KEY_SIX_SIZE, duDir4.getSize());
  }

  @Test
  public void testDiskUsageKey() throws Exception {
    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_PATH,
        false, false, false);
    DUResponse keyObj = (DUResponse) keyResponse.getEntity();
    assertEquals(0, keyObj.getCount());
    assertEquals(KEY_FOUR_SIZE, keyObj.getSize());
  }

  @Test
  public void testDiskUsageUnknown() throws Exception {
    // invalid path check
    Response invalidResponse = nsSummaryEndpoint.getDiskUsage(INVALID_PATH,
        false, false, false);
    DUResponse invalidObj = (DUResponse) invalidResponse.getEntity();
    assertEquals(ResponseStatus.PATH_NOT_FOUND,
        invalidObj.getStatus());
  }

  @Test
  public void testDiskUsageWithReplication() throws Exception {
    setUpMultiBlockKey();
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(MULTI_BLOCK_KEY_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_KEY_SIZE_WITH_REPLICA,
        replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderRootWithReplication() throws IOException {
    //   withReplica is true
    Response rootResponse = nsSummaryEndpoint.getDiskUsage(ROOT_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) rootResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_ROOT,
        replicaDUResponse.getSizeWithReplica());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_VOL,
        replicaDUResponse.getDuData().get(0).getSizeWithReplica());

  }

  @Test
  public void testDataSizeUnderVolWithReplication() throws IOException {
    Response volResponse = nsSummaryEndpoint.getDiskUsage(VOL_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) volResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_VOL,
        replicaDUResponse.getSizeWithReplica());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_BUCKET1,
        replicaDUResponse.getDuData().get(0).getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderBucketWithReplication() throws IOException {
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_ONE_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) bucketResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_BUCKET1,
        replicaDUResponse.getSizeWithReplica());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_DIR1,
        replicaDUResponse.getDuData().get(0).getSizeWithReplica());
  }

  /**
   * When calculating DU under dir1
   * there are 3 keys, file2, file3, file6.
   * There is one direct key, file7.
   * @throws IOException
   */
  @Test
  public void testDataSizeUnderDirWithReplication() throws IOException {
    Response dir1Response = nsSummaryEndpoint.getDiskUsage(DIR_ONE_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) dir1Response.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_DIR1,
        replicaDUResponse.getSizeWithReplica());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_DIR2,
        replicaDUResponse.getDuData().get(0).getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderKeyWithReplication() throws IOException {
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_KEY,
        replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testQuotaUsage() throws Exception {
    // root level quota usage
    Response rootResponse = nsSummaryEndpoint.getQuotaUsage(ROOT_PATH);
    QuotaUsageResponse quRootRes =
        (QuotaUsageResponse) rootResponse.getEntity();
    assertEquals(ROOT_QUOTA, quRootRes.getQuota());
    assertEquals(ROOT_DATA_SIZE, quRootRes.getQuotaUsed());

    // volume level quota usage
    Response volResponse = nsSummaryEndpoint.getQuotaUsage(VOL_PATH);
    QuotaUsageResponse quVolRes = (QuotaUsageResponse) volResponse.getEntity();
    assertEquals(VOL_QUOTA, quVolRes.getQuota());
    assertEquals(VOL_DATA_SIZE, quVolRes.getQuotaUsed());

    // bucket level quota usage
    Response bucketRes = nsSummaryEndpoint.getQuotaUsage(BUCKET_ONE_PATH);
    QuotaUsageResponse quBucketRes = (QuotaUsageResponse) bucketRes.getEntity();
    assertEquals(BUCKET_ONE_QUOTA, quBucketRes.getQuota());
    assertEquals(BUCKET_ONE_DATA_SIZE, quBucketRes.getQuotaUsed());

    Response bucketRes2 = nsSummaryEndpoint.getQuotaUsage(BUCKET_TWO_PATH);
    QuotaUsageResponse quBucketRes2 =
        (QuotaUsageResponse) bucketRes2.getEntity();
    assertEquals(BUCKET_TWO_QUOTA, quBucketRes2.getQuota());
    assertEquals(BUCKET_TWO_DATA_SIZE, quBucketRes2.getQuotaUsed());

    // other level not applicable
    Response naResponse1 = nsSummaryEndpoint.getQuotaUsage(DIR_ONE_PATH);
    QuotaUsageResponse quotaUsageResponse1 =
        (QuotaUsageResponse) naResponse1.getEntity();
    assertEquals(ResponseStatus.TYPE_NOT_APPLICABLE,
        quotaUsageResponse1.getResponseCode());

    Response naResponse2 = nsSummaryEndpoint.getQuotaUsage(KEY_PATH);
    QuotaUsageResponse quotaUsageResponse2 =
        (QuotaUsageResponse) naResponse2.getEntity();
    assertEquals(ResponseStatus.TYPE_NOT_APPLICABLE,
        quotaUsageResponse2.getResponseCode());

    // invalid path request
    Response invalidRes = nsSummaryEndpoint.getQuotaUsage(INVALID_PATH);
    QuotaUsageResponse invalidResObj =
        (QuotaUsageResponse) invalidRes.getEntity();
    assertEquals(ResponseStatus.PATH_NOT_FOUND,
        invalidResObj.getResponseCode());
  }

  @Test
  public void testFileSizeDist() throws Exception {
    checkFileSizeDist(ROOT_PATH, 5, 3, 4, 2);
    checkFileSizeDist(VOL_PATH, 2, 1, 2, 2);
    checkFileSizeDist(BUCKET_ONE_PATH, 1, 1, 1, 2);
    checkFileSizeDist(DIR_ONE_PATH, 0, 1, 1, 2);
  }

  public void checkFileSizeDist(String path, int bin0,
      int bin1, int bin2, int bin3) throws Exception {
    Response res = nsSummaryEndpoint.getFileSizeDistribution(path);
    FileSizeDistributionResponse fileSizeDistResObj =
            (FileSizeDistributionResponse) res.getEntity();
    int[] fileSizeDist = fileSizeDistResObj.getFileSizeDist();
    assertEquals(bin0, fileSizeDist[0]);
    assertEquals(bin1, fileSizeDist[1]);
    assertEquals(bin2, fileSizeDist[2]);
    assertEquals(bin3, fileSizeDist[3]);
    for (int i = 4; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
      assertEquals(0, fileSizeDist[i]);
    }
  }

  @Test
  public void testConstructFullPath() throws IOException {
    // For Key Tables the parent object ID is not set hence it
    // will by default be set as -1 when the NSSummary object is created
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setKeyName("dir1/dir2/file2")
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(KEY_TWO_OBJECT_ID)
        .build();
    // Call constructFullPath and verify the result
    String fullPath = ReconUtils.constructFullPath(keyInfo,
        reconNamespaceSummaryManager);
    String expectedPath = "vol/bucket1/dir1/dir2/file2";
    Assertions.assertEquals(expectedPath, fullPath);

    // Create key info for file 3
    keyInfo = new OmKeyInfo.Builder()
        .setKeyName("dir1/dir2/")
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(DIR_TWO_OBJECT_ID)
        .build();
    fullPath = ReconUtils.constructFullPath(keyInfo,
        reconNamespaceSummaryManager);
    expectedPath = "vol/bucket1/dir1/dir2/";
    Assertions.assertEquals(expectedPath, fullPath);

    // Create key info for file 6
    keyInfo = new OmKeyInfo.Builder()
        .setKeyName("dir1/dir4/file6")
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(KEY_SIX_OBJECT_ID)
        .build();
    fullPath = ReconUtils.constructFullPath(keyInfo,
        reconNamespaceSummaryManager);
    expectedPath = "vol/bucket1/dir1/dir4/file6";
    Assertions.assertEquals(expectedPath, fullPath);
  }

  @Test
  public void testDataSizeUnderVolumeWithRatisReplication()throws IOException {
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(VOL_THREE_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(QuotaUtil.getReplicatedSize(replicaDUResponse.getSize(), RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE)), replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderBucketWithRatisReplication()throws IOException {
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(VOL_THREE_PATH + "/" + BUCKET_FIVE,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(QuotaUtil.getReplicatedSize(replicaDUResponse.getSize(), RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE)), replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderDirWithRatisReplication()throws IOException {
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(VOL_THREE_PATH + "/" + BUCKET_FIVE + "/" + DIR_SIX,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(QuotaUtil.getReplicatedSize(replicaDUResponse.getSize(), RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE)), replicaDUResponse.getSizeWithReplica());
  }

  /**
   * Write directories and keys info into OM DB.
   * @throws Exception
   */
  @SuppressWarnings("checkstyle:MethodLength")
  private void populateOMDB() throws Exception {
    // write all directories
    writeDirToOm(reconOMMetadataManager,
          (DIR_ONE + OM_KEY_PREFIX),
          BUCKET_ONE,
          VOL,
          DIR_ONE,
          DIR_ONE_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          getBucketLayout());
    writeDirToOm(reconOMMetadataManager,
          (DIR_ONE + OM_KEY_PREFIX + DIR_TWO + OM_KEY_PREFIX),
          BUCKET_ONE,
          VOL,
          DIR_TWO,
          DIR_TWO_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          getBucketLayout());
    writeDirToOm(reconOMMetadataManager,
          (DIR_ONE + OM_KEY_PREFIX + DIR_THREE + OM_KEY_PREFIX),
          BUCKET_ONE,
          VOL,
          DIR_THREE,
          DIR_THREE_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          getBucketLayout());
    writeDirToOm(reconOMMetadataManager,
          (DIR_ONE + OM_KEY_PREFIX + DIR_FOUR + OM_KEY_PREFIX),
          BUCKET_ONE,
          VOL,
          DIR_FOUR,
          DIR_FOUR_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          getBucketLayout());
    writeDirToOm(reconOMMetadataManager,
          (DIR_FIVE + OM_KEY_PREFIX),
          BUCKET_THREE,
          VOL_TWO,
          DIR_FIVE,
          DIR_FIVE_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_THREE_OBJECT_ID,
          VOL_TWO_OBJECT_ID,
          getBucketLayout());

    // write all keys
    writeKeyToOm(reconOMMetadataManager,
          KEY_ONE,
          BUCKET_ONE,
          VOL,
          FILE_ONE,
          KEY_ONE_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          KEY_ONE_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_TWO,
          BUCKET_ONE,
          VOL,
          FILE_TWO,
          KEY_TWO_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          KEY_TWO_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_THREE,
          BUCKET_ONE,
          VOL,
          FILE_THREE,
          KEY_THREE_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          KEY_THREE_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_FOUR,
          BUCKET_TWO,
          VOL,
          FILE_FOUR,
          KEY_FOUR_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_TWO_OBJECT_ID,
          VOL_OBJECT_ID,
          KEY_FOUR_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_FIVE,
          BUCKET_TWO,
          VOL,
          FILE_FIVE,
          KEY_FIVE_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_TWO_OBJECT_ID,
          VOL_OBJECT_ID,
          KEY_FIVE_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_SIX,
          BUCKET_ONE,
          VOL,
          FILE_SIX,
          KEY_SIX_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_ONE_OBJECT_ID,
          VOL_OBJECT_ID,
          KEY_SIX_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_EIGHT,
          BUCKET_THREE,
          VOL_TWO,
          FILE_EIGHT,
          KEY_EIGHT_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_THREE_OBJECT_ID,
          VOL_TWO_OBJECT_ID,
          KEY_EIGHT_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_NINE,
          BUCKET_THREE,
          VOL_TWO,
          FILE_NINE,
          KEY_NINE_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_THREE_OBJECT_ID,
          VOL_TWO_OBJECT_ID,
          KEY_NINE_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_TEN,
          BUCKET_THREE,
          VOL_TWO,
          FILE_TEN,
          KEY_TEN_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_THREE_OBJECT_ID,
          VOL_TWO_OBJECT_ID,
          KEY_TEN_SIZE,
          getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
          KEY_ELEVEN,
          BUCKET_FOUR,
          VOL_TWO,
          FILE_ELEVEN,
          KEY_ELEVEN_OBJECT_ID,
          PARENT_OBJECT_ID_ZERO,
          BUCKET_FOUR_OBJECT_ID,
          VOL_TWO_OBJECT_ID,
          KEY_ELEVEN_SIZE,
          getBucketLayout());
  }

  private void populateVolumeThree() throws IOException {

    writeDirToOm(reconOMMetadataManager,
        (DIR_SIX + OM_KEY_PREFIX),
        BUCKET_FIVE,
        VOL_THREE,
        DIR_SIX,
        DIR_SIX_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID,
        getBucketLayout());

    writeDirToOm(reconOMMetadataManager,
        (DIR_SIX + OM_KEY_PREFIX + DIR_SEVEN + OM_KEY_PREFIX),
        BUCKET_FIVE,
        VOL_THREE,
        DIR_SEVEN,
        DIR_SEVEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID,
        getBucketLayout());

    writeKeyToOm(reconOMMetadataManager,
        KEY_TWELVE,
        BUCKET_FIVE,
        VOL_THREE,
        FILE_TWELVE,
        FILE_TWELVE_OBJECT_ID,
        BUCKET_FIVE_OBJECT_ID,
        BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID,
        KEY_TWELVE_SIZE,
        getBucketLayout(), RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        Time.now(), true);

    writeKeyToOm(reconOMMetadataManager,
        KEY_THIRTEEN,
        BUCKET_FIVE,
        VOL_THREE,
        FILE_THIRTEEN,
        FILE_THIRTEEN_OBJECT_ID,
        DIR_SIX_OBJECT_ID,
        BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID,
        KEY_THIRTEEN_SIZE,
        getBucketLayout(), RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        Time.now(), true);

    writeKeyToOm(reconOMMetadataManager,
        KEY_FOURTEEN,
        BUCKET_FIVE,
        VOL_THREE,
        FILE_FOURTEEN,
        FILE_FOURTEEN_OBJECT_ID,
        DIR_SEVEN_OBJECT_ID,
        BUCKET_FIVE_OBJECT_ID,
        VOL_THREE_OBJECT_ID,
        KEY_FOURTEEN_SIZE,
        getBucketLayout(), RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        Time.now(), true);
  }

  /**
   * Create a new OM Metadata manager instance with one user, one vol, and two
   * buckets.
   * @throws IOException ioEx
   */
  private static OMMetadataManager initializeNewOmMetadataManager(
      File omDbDir, OzoneConfiguration omConfiguration)
      throws IOException {
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    omConfiguration.set(OMConfigKeys
        .OZONE_OM_ENABLE_FILESYSTEM_PATHS, "true");
    setConfiguration(omConfiguration);
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
        omConfiguration, null);

    String volumeKey = omMetadataManager.getVolumeKey(VOL);
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setObjectID(VOL_OBJECT_ID)
            .setVolume(VOL)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .setQuotaInBytes(VOL_QUOTA)
            .build();

    String volume2Key = omMetadataManager.getVolumeKey(VOL_TWO);
    OmVolumeArgs args2 =
        OmVolumeArgs.newBuilder()
            .setObjectID(VOL_TWO_OBJECT_ID)
            .setVolume(VOL_TWO)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .setQuotaInBytes(VOL_TWO_QUOTA)
            .build();

    String volume3Key = omMetadataManager.getVolumeKey(VOL_THREE);
    OmVolumeArgs args3 =
        OmVolumeArgs.newBuilder()
            .setObjectID(VOL_THREE_OBJECT_ID)
            .setVolume(VOL_THREE)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .setQuotaInBytes(VOL_THREE_QUOTA)
            .build();

    omMetadataManager.getVolumeTable().put(volumeKey, args);
    omMetadataManager.getVolumeTable().put(volume2Key, args2);
    omMetadataManager.getVolumeTable().put(volume3Key, args3);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(BUCKET_ONE_OBJECT_ID)
        .setQuotaInBytes(BUCKET_ONE_QUOTA)
        .setBucketLayout(getBucketLayout())
        .build();

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(BUCKET_TWO_OBJECT_ID)
        .setQuotaInBytes(BUCKET_TWO_QUOTA)
        .setBucketLayout(getBucketLayout())
        .build();

    OmBucketInfo bucketInfo3 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL_TWO)
        .setBucketName(BUCKET_THREE)
        .setObjectID(BUCKET_THREE_OBJECT_ID)
        .setQuotaInBytes(BUCKET_THREE_QUOTA)
        .setBucketLayout(getBucketLayout())
        .build();

    OmBucketInfo bucketInfo4 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL_TWO)
        .setBucketName(BUCKET_FOUR)
        .setObjectID(BUCKET_FOUR_OBJECT_ID)
        .setQuotaInBytes(BUCKET_FOUR_QUOTA)
        .setBucketLayout(getBucketLayout())
        .build();

    OmBucketInfo bucketInfo5 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL_THREE)
        .setBucketName(BUCKET_FIVE)
        .setObjectID(BUCKET_FIVE_OBJECT_ID)
        .setQuotaInBytes(BUCKET_FIVE_QUOTA)
        .setBucketLayout(getBucketLayout())
        .build();

    String bucketKey = omMetadataManager.getBucketKey(
        bucketInfo.getVolumeName(), bucketInfo.getBucketName());
    String bucketKey2 = omMetadataManager.getBucketKey(
        bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());
    String bucketKey3 = omMetadataManager.getBucketKey(
        bucketInfo3.getVolumeName(), bucketInfo3.getBucketName());
    String bucketKey4 = omMetadataManager.getBucketKey(
        bucketInfo4.getVolumeName(), bucketInfo4.getBucketName());
    String bucketKey5 = omMetadataManager.getBucketKey(
        bucketInfo5.getVolumeName(), bucketInfo5.getBucketName());

    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo);
    omMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);
    omMetadataManager.getBucketTable().put(bucketKey3, bucketInfo3);
    omMetadataManager.getBucketTable().put(bucketKey4, bucketInfo4);
    omMetadataManager.getBucketTable().put(bucketKey5, bucketInfo5);

    return omMetadataManager;
  }

  private void setUpMultiBlockKey() throws IOException {
    OmKeyLocationInfoGroup locationInfoGroup =
        getLocationInfoGroup1();

    // add the multi-block key to Recon's OM
    writeKeyToOm(reconOMMetadataManager,
        MULTI_BLOCK_KEY,
        BUCKET_ONE,
        VOL,
        MULTI_BLOCK_FILE,
        MULTI_BLOCK_KEY_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        getBucketLayout(),
        KEY_SEVEN_SIZE);
  }

  private OmKeyLocationInfoGroup getLocationInfoGroup1() {
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

    return new OmKeyLocationInfoGroup(0L, locationInfoList);
  }

  /**
   * Testing the following case.
   *                     vol
   *               /             \
   *        bucket1               bucket2
   *        /    \                /     \
   *     file1      dir1        file4  file5
   *           /   \   \     \
   *        dir2  dir3  dir4  file7
   *         /     \      \
   *       file2   file3  file6
   *  ----------------------------------------
   *                  vol2
   *              /         \
   *      bucket3          bucket4
   *      /      \           /
   *   file8     dir5      file11
   *            /    \
   *        file9    file10
   * Write these keys to OM and
   * replicate them.
   */
  private OmKeyLocationInfoGroup getLocationInfoGroup2() {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    BlockID block4 = new BlockID(CONTAINER_FOUR_ID, 0L);
    BlockID block5 = new BlockID(CONTAINER_FIVE_ID, 0L);
    BlockID block6 = new BlockID(CONTAINER_SIX_ID, 0L);

    OmKeyLocationInfo location4 = new OmKeyLocationInfo.Builder()
        .setBlockID(block4)
        .setLength(BLOCK_FOUR_LENGTH)
        .build();
    OmKeyLocationInfo location5 = new OmKeyLocationInfo.Builder()
        .setBlockID(block5)
        .setLength(BLOCK_FIVE_LENGTH)
        .build();
    OmKeyLocationInfo location6 = new OmKeyLocationInfo.Builder()
        .setBlockID(block6)
        .setLength(BLOCK_SIX_LENGTH)
        .build();
    locationInfoList.add(location4);
    locationInfoList.add(location5);
    locationInfoList.add(location6);
    return new OmKeyLocationInfoGroup(0L, locationInfoList);

  }

  @SuppressWarnings("checkstyle:MethodLength")
  private void setUpMultiBlockReplicatedKeys() throws IOException {
    OmKeyLocationInfoGroup locationInfoGroup1 =
        getLocationInfoGroup1();
    OmKeyLocationInfoGroup locationInfoGroup2 =
        getLocationInfoGroup2();

    //vol/bucket1/file1
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        FILE_ONE,
        KEY_ONE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
          getBucketLayout(),
          KEY_ONE_SIZE);

    //vol/bucket1/dir1/dir2/file2
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_ONE,
        VOL,
        FILE_TWO,
        KEY_TWO_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
          getBucketLayout(),
          KEY_TWO_SIZE);

    //vol/bucket1/dir1/dir3/file3
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        FILE_THREE,
        KEY_THREE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
          getBucketLayout(),
          KEY_THREE_SIZE);

    //vol/bucket2/file4
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        FILE_FOUR,
        KEY_FOUR_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getBucketLayout(),
        KEY_FOUR_SIZE);

    //vol/bucket2/file5
    writeKeyToOm(reconOMMetadataManager,
        KEY_FIVE,
        BUCKET_TWO,
        VOL,
        FILE_FIVE,
        KEY_FIVE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getBucketLayout(),
        KEY_FIVE_SIZE);

    //vol/bucket1/dir1/dir4/file6
    writeKeyToOm(reconOMMetadataManager,
        KEY_SIX,
        BUCKET_ONE,
        VOL,
        FILE_SIX,
        KEY_SIX_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getBucketLayout(),
        KEY_SIX_SIZE);

    //vol/bucket1/dir1/file7
    writeKeyToOm(reconOMMetadataManager,
        KEY_SEVEN,
        BUCKET_ONE,
        VOL,
        FILE_SEVEN,
        KEY_SEVEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getBucketLayout(),
        KEY_SEVEN_SIZE);

    //vol2/bucket3/file8
    writeKeyToOm(reconOMMetadataManager,
        KEY_EIGHT,
        BUCKET_THREE,
        VOL_TWO,
        FILE_EIGHT,
        KEY_EIGHT_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getBucketLayout(),
        KEY_EIGHT_SIZE);

    //vol2/bucket3/dir5/file9
    writeKeyToOm(reconOMMetadataManager,
        KEY_NINE,
        BUCKET_THREE,
        VOL_TWO,
        FILE_NINE,
        KEY_NINE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getBucketLayout(),
        KEY_NINE_SIZE);

    //vol2/bucket3/dir5/file10
    writeKeyToOm(reconOMMetadataManager,
        KEY_TEN,
        BUCKET_THREE,
        VOL_TWO,
        FILE_TEN,
        KEY_TEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getBucketLayout(),
        KEY_TEN_SIZE);

    //vol2/bucket4/file11
    writeKeyToOm(reconOMMetadataManager,
        KEY_ELEVEN,
        BUCKET_FOUR,
        VOL_TWO,
        FILE_ELEVEN,
        KEY_ELEVEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_FOUR_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getBucketLayout(),
        KEY_ELEVEN_SIZE);
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
    ContainerID containerID1 = ContainerID.valueOf(CONTAINER_ONE_ID);
    Set<ContainerReplica> containerReplicas1 = generateMockContainerReplicas(
        CONTAINER_ONE_REPLICA_COUNT, containerID1);
    when(containerManager.getContainerReplicas(containerID1))
        .thenReturn(containerReplicas1);

    // Container 2 is under replicated with 2 replica
    ContainerID containerID2 = ContainerID.valueOf(CONTAINER_TWO_ID);
    Set<ContainerReplica> containerReplicas2 = generateMockContainerReplicas(
        CONTAINER_TWO_REPLICA_COUNT, containerID2);
    when(containerManager.getContainerReplicas(containerID2))
        .thenReturn(containerReplicas2);

    // Container 3 is over replicated with 4 replica
    ContainerID containerID3 = ContainerID.valueOf(CONTAINER_THREE_ID);
    Set<ContainerReplica> containerReplicas3 = generateMockContainerReplicas(
        CONTAINER_THREE_REPLICA_COUNT, containerID3);
    when(containerManager.getContainerReplicas(containerID3))
        .thenReturn(containerReplicas3);

    // Container 4 is replicated with 5 replica
    ContainerID containerID4 = ContainerID.valueOf(CONTAINER_FOUR_ID);
    Set<ContainerReplica> containerReplicas4 = generateMockContainerReplicas(
        CONTAINER_FOUR_REPLICA_COUNT, containerID4);
    when(containerManager.getContainerReplicas(containerID4))
        .thenReturn(containerReplicas4);

    // Container 5 is replicated with 2 replica
    ContainerID containerID5 = ContainerID.valueOf(CONTAINER_FIVE_ID);
    Set<ContainerReplica> containerReplicas5 = generateMockContainerReplicas(
        CONTAINER_FIVE_REPLICA_COUNT, containerID5);
    when(containerManager.getContainerReplicas(containerID5))
        .thenReturn(containerReplicas5);

    // Container 6 is replicated with 3 replica
    ContainerID containerID6 = ContainerID.valueOf(CONTAINER_SIX_ID);
    Set<ContainerReplica> containerReplicas6 = generateMockContainerReplicas(
        CONTAINER_SIX_REPLICA_COUNT, containerID6);
    when(containerManager.getContainerReplicas(containerID6))
        .thenReturn(containerReplicas6);

    when(reconSCM.getContainerManager()).thenReturn(containerManager);
    ReconNodeManager mockReconNodeManager = mock(ReconNodeManager.class);
    when(mockReconNodeManager.getStats()).thenReturn(getMockSCMRootStat());
    when(reconSCM.getScmNodeManager()).thenReturn(mockReconNodeManager);
    return reconSCM;
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.LEGACY;
  }

  private static SCMNodeStat getMockSCMRootStat() {
    return new SCMNodeStat(ROOT_QUOTA, ROOT_DATA_SIZE, 
        ROOT_QUOTA - ROOT_DATA_SIZE, 0, ROOT_QUOTA - ROOT_DATA_SIZE - 1, 0);
  }
}
