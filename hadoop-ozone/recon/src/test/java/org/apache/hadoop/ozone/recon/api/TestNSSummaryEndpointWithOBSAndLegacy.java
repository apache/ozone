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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.helpers.QuotaUtil.getReplicatedSize;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
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
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.OmUtils;
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
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithLegacy;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithOBS;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the NSSummary REST APIs within the context of an Object Store (OBS) layout,
 * as well as Legacy layout buckets with FileSystemPaths disabled. The tests aim to
 * validate API responses for buckets that follow the flat hierarchy model typical
 * of OBS layouts.
 * <p>
 * The test environment simulates a simple object storage structure with volumes
 * containing buckets, which in turn contain files. Specifically, it includes:
 * - Two OBS layout buckets (bucket1 and bucket2) under 'vol', each containing
 * multiple files.
 * - Two Legacy layout buckets (bucket3 and bucket4) under 'vol2', with 'bucket4'
 * the fileSystemEnabled flag set to false for these legacy buckets.
 * <p>
 * The directory structure for testing is as follows:
 * .
 * └── vol
 *     ├── bucket1 (OBS)
 *     │   ├── KEY_ONE
 *     │   ├── KEY_TWO
 *     │   └── KEY_THREE
 *     └── bucket2 (OBS)
 *         ├── KEY_FOUR
 *         └── KEY_FIVE
 * └── vol2
 *     ├── bucket3 (Legacy)
 *     │   ├── KEY_EIGHT
 *     │   ├── KEY_NINE
 *     │   └── KEY_TEN
 *     └── bucket4 (Legacy)
 *         └── KEY_ELEVEN
 */
public class TestNSSummaryEndpointWithOBSAndLegacy extends NSSummaryTests {
  @TempDir
  private Path temporaryFolder;

  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
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
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String BUCKET_THREE = "bucket3";
  private static final String BUCKET_FOUR = "bucket4";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "////file2";
  private static final String KEY_THREE = "file3///";
  private static final String KEY_FOUR = "file4";
  private static final String KEY_FIVE = "_//////";
  private static final String KEY_EIGHT = "file8";
  private static final String KEY_NINE = "//////";
  private static final String KEY_TEN = "///__file10";
  private static final String KEY_ELEVEN = "////file11";
  private static final String MULTI_BLOCK_FILE = KEY_THREE;

  private static final long PARENT_OBJECT_ID_ZERO = 0L;
  private static final long VOL_OBJECT_ID = 0L;
  private static final long VOL_TWO_OBJECT_ID = 14L;
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long BUCKET_THREE_OBJECT_ID = 15L;
  private static final long BUCKET_FOUR_OBJECT_ID = 16L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long KEY_FOUR_OBJECT_ID = 6L;
  private static final long KEY_FIVE_OBJECT_ID = 9L;
  private static final long KEY_EIGHT_OBJECT_ID = 17L;
  private static final long KEY_NINE_OBJECT_ID = 19L;
  private static final long KEY_TEN_OBJECT_ID = 20L;
  private static final long KEY_ELEVEN_OBJECT_ID = 21L;
  private static final long MULTI_BLOCK_KEY_OBJECT_ID = 13L;

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
  private static final long FILE_ONE_SIZE = 500L; // bin 0
  private static final long FILE_TWO_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long FILE_THREE_SIZE = 4 * OzoneConsts.KB + 1; // bin 3
  private static final long FILE_FOUR_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long FILE_FIVE_SIZE = 100L; // bin 0
  private static final long FILE_EIGHT_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long FILE_NINE_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long FILE_TEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long FILE_ELEVEN_SIZE = OzoneConsts.KB + 1; // bin 1

  private static final long FILE1_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_ONE_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE2_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_TWO_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE3_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_THREE_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE4_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_FOUR_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE5_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_FIVE_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));

  private static final long FILE8_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_EIGHT_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE9_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_NINE_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE10_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_TEN_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));
  private static final long FILE11_SIZE_WITH_REPLICA =
      getReplicatedSize(FILE_ELEVEN_SIZE,
          StandaloneReplicationConfig.getInstance(ONE));

  private static final long MULTI_BLOCK_KEY_SIZE_WITH_REPLICA
      = FILE3_SIZE_WITH_REPLICA;
  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_ROOT
      = FILE1_SIZE_WITH_REPLICA
      + FILE2_SIZE_WITH_REPLICA
      + FILE3_SIZE_WITH_REPLICA
      + FILE4_SIZE_WITH_REPLICA
      + FILE5_SIZE_WITH_REPLICA
      + FILE8_SIZE_WITH_REPLICA
      + FILE9_SIZE_WITH_REPLICA
      + FILE10_SIZE_WITH_REPLICA
      + FILE11_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_VOL
      = FILE1_SIZE_WITH_REPLICA
      + FILE2_SIZE_WITH_REPLICA
      + FILE3_SIZE_WITH_REPLICA
      + FILE4_SIZE_WITH_REPLICA
      + FILE5_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_BUCKET1
      = FILE1_SIZE_WITH_REPLICA
      + FILE2_SIZE_WITH_REPLICA
      + FILE3_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_BUCKET3
      = FILE8_SIZE_WITH_REPLICA +
      FILE9_SIZE_WITH_REPLICA +
      FILE10_SIZE_WITH_REPLICA;

  private static final long
      MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_KEY
      = FILE4_SIZE_WITH_REPLICA;

  // quota in bytes
  private static final long ROOT_QUOTA = 2 * (2 * OzoneConsts.MB);
  private static final long VOL_QUOTA = 2 * OzoneConsts.MB;
  private static final long VOL_TWO_QUOTA = 2 * OzoneConsts.MB;
  private static final long BUCKET_ONE_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_TWO_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_THREE_QUOTA = OzoneConsts.MB;
  private static final long BUCKET_FOUR_QUOTA = OzoneConsts.MB;

  // mock client's path requests
  private static final String TEST_USER = "TestUser";
  private static final String ROOT_PATH = "/";
  private static final String VOL_PATH = ROOT_PATH + VOL;
  private static final String VOL_TWO_PATH = ROOT_PATH + VOL_TWO;
  private static final String BUCKET_ONE_PATH =
      ROOT_PATH + VOL + ROOT_PATH + BUCKET_ONE;
  private static final String BUCKET_TWO_PATH =
      ROOT_PATH + VOL + ROOT_PATH + BUCKET_TWO;
  private static final String BUCKET_THREE_PATH =
      ROOT_PATH + VOL_TWO + ROOT_PATH + BUCKET_THREE;
  private static final String BUCKET_FOUR_PATH =
      ROOT_PATH + VOL_TWO + ROOT_PATH + BUCKET_FOUR;
  private static final String KEY_ONE_PATH =
      ROOT_PATH + VOL + ROOT_PATH + BUCKET_ONE + ROOT_PATH + KEY_ONE;
  private static final String KEY_TWO_PATH =
      ROOT_PATH + VOL + ROOT_PATH + BUCKET_ONE + ROOT_PATH + KEY_TWO;
  private static final String KEY_FIVE_PATH =
      ROOT_PATH + VOL + ROOT_PATH + BUCKET_TWO + ROOT_PATH + KEY_FIVE;
  private static final String KEY_EIGHT_PATH =
      ROOT_PATH + VOL_TWO + ROOT_PATH + BUCKET_THREE + ROOT_PATH + KEY_EIGHT;
  private static final String KEY_ELEVEN_PATH =
      ROOT_PATH + VOL_TWO + ROOT_PATH + BUCKET_FOUR + ROOT_PATH + KEY_ELEVEN;
  private static final String KEY4_PATH =
      ROOT_PATH + VOL + ROOT_PATH + BUCKET_TWO + ROOT_PATH + KEY_FOUR;
  private static final String MULTI_BLOCK_KEY_PATH =
      ROOT_PATH + VOL + ROOT_PATH + BUCKET_ONE + ROOT_PATH + KEY_THREE;
  private static final String INVALID_PATH = "/vol/path/not/found";

  // some expected answers
  private static final long ROOT_DATA_SIZE =
      FILE_ONE_SIZE + FILE_TWO_SIZE + FILE_THREE_SIZE + FILE_FOUR_SIZE +
          FILE_FIVE_SIZE + FILE_EIGHT_SIZE + FILE_NINE_SIZE + FILE_TEN_SIZE +
          FILE_ELEVEN_SIZE;
  private static final long VOL_DATA_SIZE = FILE_ONE_SIZE + FILE_TWO_SIZE +
      FILE_THREE_SIZE + FILE_FOUR_SIZE + FILE_FIVE_SIZE;

  private static final long VOL_TWO_DATA_SIZE =
      FILE_EIGHT_SIZE + FILE_NINE_SIZE + FILE_TEN_SIZE + FILE_ELEVEN_SIZE;

  private static final long BUCKET_ONE_DATA_SIZE = FILE_ONE_SIZE +
      FILE_TWO_SIZE +
      FILE_THREE_SIZE;

  private static final long BUCKET_TWO_DATA_SIZE =
      FILE_FOUR_SIZE + FILE_FIVE_SIZE;

  private static final long BUCKET_THREE_DATA_SIZE =
      FILE_EIGHT_SIZE + FILE_NINE_SIZE + FILE_TEN_SIZE;

  private static final long BUCKET_FOUR_DATA_SIZE = FILE_ELEVEN_SIZE;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // By setting this config our Legacy buckets will behave like OBS buckets.
    conf.set(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, "false");
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
    reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    nsSummaryEndpoint = reconTestInjector.getInstance(NSSummaryEndpoint.class);

    // populate OM DB and reprocess into Recon RocksDB
    populateOMDB();
    NSSummaryTaskWithOBS nsSummaryTaskWithOBS =
        new NSSummaryTaskWithOBS(reconNamespaceSummaryManager,
            reconOMMetadataManager, 10, 5, 20, 2000);
    nsSummaryTaskWithOBS.reprocessWithOBS(reconOMMetadataManager);
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
    // Test root basics
    Response rootResponse = nsSummaryEndpoint.getBasicInfo(ROOT_PATH);
    NamespaceSummaryResponse rootResponseObj =
        (NamespaceSummaryResponse) rootResponse.getEntity();
    assertEquals(EntityType.ROOT, rootResponseObj.getEntityType());
    assertEquals(2, rootResponseObj.getCountStats().getNumVolume());
    assertEquals(4, rootResponseObj.getCountStats().getNumBucket());
    assertEquals(9, rootResponseObj.getCountStats().getNumTotalKey());
  }

  @Test
  public void testGetBasicInfoVol() throws Exception {
    // Test volume basics
    Response volResponse = nsSummaryEndpoint.getBasicInfo(VOL_PATH);
    NamespaceSummaryResponse volResponseObj =
        (NamespaceSummaryResponse) volResponse.getEntity();
    assertEquals(EntityType.VOLUME,
        volResponseObj.getEntityType());
    assertEquals(2, volResponseObj.getCountStats().getNumBucket());
    assertEquals(5, volResponseObj.getCountStats().getNumTotalKey());
    assertEquals(TEST_USER, ((VolumeObjectDBInfo) volResponseObj.
        getObjectDBInfo()).getAdmin());
    assertEquals(TEST_USER, ((VolumeObjectDBInfo) volResponseObj.
        getObjectDBInfo()).getOwner());
    assertEquals(VOL, volResponseObj.getObjectDBInfo().getName());
    assertEquals(2097152, volResponseObj.getObjectDBInfo().getQuotaInBytes());
    assertEquals(-1, volResponseObj.getObjectDBInfo().getQuotaInNamespace());
  }

  @Test
  public void testGetBasicInfoVolTwo() throws Exception {
    // Test volume 2's basics
    Response volTwoResponse = nsSummaryEndpoint.getBasicInfo(VOL_TWO_PATH);
    NamespaceSummaryResponse volTwoResponseObj =
        (NamespaceSummaryResponse) volTwoResponse.getEntity();
    assertEquals(EntityType.VOLUME,
        volTwoResponseObj.getEntityType());
    assertEquals(2, volTwoResponseObj.getCountStats().getNumBucket());
    assertEquals(4, volTwoResponseObj.getCountStats().getNumTotalKey());
    assertEquals(TEST_USER, ((VolumeObjectDBInfo) volTwoResponseObj.
        getObjectDBInfo()).getAdmin());
    assertEquals(TEST_USER, ((VolumeObjectDBInfo) volTwoResponseObj.
        getObjectDBInfo()).getOwner());
    assertEquals(VOL_TWO, volTwoResponseObj.getObjectDBInfo().getName());
    assertEquals(2097152,
        volTwoResponseObj.getObjectDBInfo().getQuotaInBytes());
    assertEquals(-1, volTwoResponseObj.getObjectDBInfo().getQuotaInNamespace());
  }

  @Test
  public void testGetBasicInfoBucketOne() throws Exception {
    // Test bucket 1's basics
    Response bucketOneResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_ONE_PATH);
    NamespaceSummaryResponse bucketOneObj =
        (NamespaceSummaryResponse) bucketOneResponse.getEntity();
    assertEquals(EntityType.BUCKET, bucketOneObj.getEntityType());
    assertEquals(3, bucketOneObj.getCountStats().getNumTotalKey());
    assertEquals(VOL,
        ((BucketObjectDBInfo) bucketOneObj.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketOneObj.getObjectDBInfo()).getStorageType());
    assertEquals(getOBSBucketLayout(),
        ((BucketObjectDBInfo)
            bucketOneObj.getObjectDBInfo()).getBucketLayout());
    assertEquals(BUCKET_ONE,
        ((BucketObjectDBInfo) bucketOneObj.getObjectDBInfo()).getName());
  }

  @Test
  public void testGetBasicInfoBucketTwo() throws Exception {
    // Test bucket 2's basics
    Response bucketTwoResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_TWO_PATH);
    NamespaceSummaryResponse bucketTwoObj =
        (NamespaceSummaryResponse) bucketTwoResponse.getEntity();
    assertEquals(EntityType.BUCKET, bucketTwoObj.getEntityType());
    assertEquals(2, bucketTwoObj.getCountStats().getNumTotalKey());
    assertEquals(VOL,
        ((BucketObjectDBInfo) bucketTwoObj.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketTwoObj.getObjectDBInfo()).getStorageType());
    assertEquals(getOBSBucketLayout(),
        ((BucketObjectDBInfo)
            bucketTwoObj.getObjectDBInfo()).getBucketLayout());
    assertEquals(BUCKET_TWO,
        ((BucketObjectDBInfo) bucketTwoObj.getObjectDBInfo()).getName());
  }

  @Test
  public void testGetBasicInfoBucketThree() throws Exception {
    // Test bucket 3's basics
    Response bucketThreeResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_THREE_PATH);
    NamespaceSummaryResponse bucketThreeObj = (NamespaceSummaryResponse)
        bucketThreeResponse.getEntity();
    assertEquals(EntityType.BUCKET, bucketThreeObj.getEntityType());
    assertEquals(3, bucketThreeObj.getCountStats().getNumTotalKey());
    assertEquals(VOL_TWO,
        ((BucketObjectDBInfo) bucketThreeObj.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketThreeObj.getObjectDBInfo()).getStorageType());
    assertEquals(getLegacyBucketLayout(),
        ((BucketObjectDBInfo)
            bucketThreeObj.getObjectDBInfo()).getBucketLayout());
    assertEquals(BUCKET_THREE,
        ((BucketObjectDBInfo) bucketThreeObj.getObjectDBInfo()).getName());
  }

  @Test
  public void testGetBasicInfoBucketFour() throws Exception {
    // Test bucket 4's basics
    Response bucketFourResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_FOUR_PATH);
    NamespaceSummaryResponse bucketFourObj =
        (NamespaceSummaryResponse) bucketFourResponse.getEntity();
    assertEquals(EntityType.BUCKET, bucketFourObj.getEntityType());
    assertEquals(1, bucketFourObj.getCountStats().getNumTotalKey());
    assertEquals(VOL_TWO,
        ((BucketObjectDBInfo) bucketFourObj.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketFourObj.getObjectDBInfo()).getStorageType());
    assertEquals(getLegacyBucketLayout(),
        ((BucketObjectDBInfo)
            bucketFourObj.getObjectDBInfo()).getBucketLayout());
    assertEquals(BUCKET_FOUR,
        ((BucketObjectDBInfo) bucketFourObj.getObjectDBInfo()).getName());
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
    assertEquals(2, duRootRes.getCount());
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
  public void testDiskUsageVolTwo() throws Exception {
    // volume level DU
    Response volResponse = nsSummaryEndpoint.getDiskUsage(VOL_TWO_PATH,
        false, false, false);
    DUResponse duVolRes = (DUResponse) volResponse.getEntity();
    assertEquals(2, duVolRes.getCount());
    List<DUResponse.DiskUsage> duData = duVolRes.getDuData();
    // sort based on subpath
    Collections.sort(duData,
        Comparator.comparing(DUResponse.DiskUsage::getSubpath));
    DUResponse.DiskUsage duBucket3 = duData.get(0);
    DUResponse.DiskUsage duBucket4 = duData.get(1);
    assertEquals(BUCKET_THREE_PATH, duBucket3.getSubpath());
    assertEquals(BUCKET_FOUR_PATH, duBucket4.getSubpath());
    assertEquals(VOL_TWO_DATA_SIZE, duVolRes.getSize());
  }

  @Test
  public void testDiskUsageBucketOne() throws Exception {
    // bucket level DU
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_ONE_PATH,
        false, false, false);
    DUResponse duBucketResponse = (DUResponse) bucketResponse.getEntity();
    // There are no sub-paths under this OBS bucket.
    assertEquals(0, duBucketResponse.getCount());

    Response bucketResponseWithSubpath = nsSummaryEndpoint.getDiskUsage(
        BUCKET_ONE_PATH, true, false, false);
    DUResponse duBucketResponseWithFiles =
        (DUResponse) bucketResponseWithSubpath.getEntity();
    assertEquals(3, duBucketResponseWithFiles.getCount());

    assertEquals(BUCKET_ONE_DATA_SIZE, duBucketResponse.getSize());
  }

  @Test
  public void testDiskUsageBucketTwo() throws Exception {
    // bucket level DU
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_TWO_PATH,
        false, false, false);
    DUResponse duBucketResponse = (DUResponse) bucketResponse.getEntity();
    // There are no sub-paths under this OBS bucket.
    assertEquals(0, duBucketResponse.getCount());

    Response bucketResponseWithSubpath = nsSummaryEndpoint.getDiskUsage(
        BUCKET_TWO_PATH, true, false, false);
    DUResponse duBucketResponseWithFiles =
        (DUResponse) bucketResponseWithSubpath.getEntity();
    assertEquals(2, duBucketResponseWithFiles.getCount());

    assertEquals(BUCKET_TWO_DATA_SIZE, duBucketResponse.getSize());
  }

  @Test
  public void testDiskUsageBucketThree() throws Exception {
    // bucket level DU
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_THREE_PATH,
        false, false, false);
    DUResponse duBucketResponse = (DUResponse) bucketResponse.getEntity();
    // There are no sub-paths under this Legacy bucket.
    assertEquals(0, duBucketResponse.getCount());

    Response bucketResponseWithSubpath = nsSummaryEndpoint.getDiskUsage(
        BUCKET_THREE_PATH, true, false, false);
    DUResponse duBucketResponseWithFiles =
        (DUResponse) bucketResponseWithSubpath.getEntity();
    assertEquals(3, duBucketResponseWithFiles.getCount());

    assertEquals(BUCKET_THREE_DATA_SIZE, duBucketResponse.getSize());
  }

  @Test
  public void testDiskUsageKey1() throws Exception {
    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_ONE_PATH,
        false, false, false);
    DUResponse duKeyResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(FILE_ONE_SIZE, duKeyResponse.getSize());
  }

  @Test
  public void testDiskUsageKey2() throws Exception {
    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_TWO_PATH,
        false, false, false);
    DUResponse duKeyResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(FILE_TWO_SIZE, duKeyResponse.getSize());
  }

  @Test
  public void testDiskUsageKey4() throws Exception {
    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY4_PATH,
        true, false, false);
    DUResponse duKeyResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(FILE_FOUR_SIZE, duKeyResponse.getSize());
  }

  @Test
  public void testDiskUsageKey5() throws Exception {
    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_FIVE_PATH,
        false, false, false);
    DUResponse duKeyResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(FILE_FIVE_SIZE, duKeyResponse.getSize());
  }

  @Test
  public void testDiskUsageKey8() throws Exception {
    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_EIGHT_PATH,
        false, false, false);
    DUResponse duKeyResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(FILE_EIGHT_SIZE, duKeyResponse.getSize());
  }

  @Test
  public void testDiskUsageKey11() throws Exception {
    // key level DU
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_ELEVEN_PATH,
        false, false, false);
    DUResponse duKeyResponse = (DUResponse) keyResponse.getEntity();
    assertEquals(0, duKeyResponse.getCount());
    assertEquals(FILE_ELEVEN_SIZE, duKeyResponse.getSize());
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
    setUpMultiBlockReplicatedKeys();
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
    setUpMultiBlockReplicatedKeys();
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
  public void testDataSizeUnderBucketOneWithReplication() throws IOException {
    setUpMultiBlockReplicatedKeys();
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_ONE_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) bucketResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_BUCKET1,
        replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderBucketThreeWithReplication() throws IOException {
    setUpMultiBlockReplicatedKeys();
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_THREE_PATH,
        false, true, false);
    DUResponse replicaDUResponse = (DUResponse) bucketResponse.getEntity();
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(MULTI_BLOCK_TOTAL_SIZE_WITH_REPLICA_UNDER_BUCKET3,
        replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderKeyWithReplication() throws IOException {
    setUpMultiBlockReplicatedKeys();
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY4_PATH,
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

    Response bucketRes3 = nsSummaryEndpoint.getQuotaUsage(BUCKET_THREE_PATH);
    QuotaUsageResponse quBucketRes3 =
        (QuotaUsageResponse) bucketRes3.getEntity();
    assertEquals(BUCKET_THREE_QUOTA, quBucketRes3.getQuota());
    assertEquals(BUCKET_THREE_DATA_SIZE, quBucketRes3.getQuotaUsed());

    Response bucketRes4 = nsSummaryEndpoint.getQuotaUsage(BUCKET_FOUR_PATH);
    QuotaUsageResponse quBucketRes4 =
        (QuotaUsageResponse) bucketRes4.getEntity();
    assertEquals(BUCKET_FOUR_QUOTA, quBucketRes4.getQuota());
    assertEquals(BUCKET_FOUR_DATA_SIZE, quBucketRes4.getQuotaUsed());

    // other level not applicable
    Response naResponse2 = nsSummaryEndpoint.getQuotaUsage(KEY4_PATH);
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
    checkFileSizeDist(ROOT_PATH, 2, 3, 3, 1);
    checkFileSizeDist(VOL_PATH, 2, 1, 1, 1);
    checkFileSizeDist(BUCKET_ONE_PATH, 1, 1, 0, 1);
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
  public void testNormalizePathUptoBucket() {
    // Test null or empty path
    assertEquals("/", OmUtils.normalizePathUptoBucket(null));
    assertEquals("/", OmUtils.normalizePathUptoBucket(""));

    // Test path with leading slashes
    assertEquals("volume1/bucket1/key1/key2",
        OmUtils.normalizePathUptoBucket("///volume1/bucket1/key1/key2"));

    // Test volume and bucket names
    assertEquals("volume1/bucket1",
        OmUtils.normalizePathUptoBucket("volume1/bucket1"));

    // Test with additional segments
    assertEquals("volume1/bucket1/key1/key2",
        OmUtils.normalizePathUptoBucket("volume1/bucket1/key1/key2"));

    // Test path with multiple slashes in key names.
    assertEquals("volume1/bucket1/key1//key2",
        OmUtils.normalizePathUptoBucket("volume1/bucket1/key1//key2"));

    // Test path with volume, bucket, and special characters in keys
    assertEquals("volume/bucket/key$%#1/./////////key$%#2",
        OmUtils.normalizePathUptoBucket("volume/bucket/key$%#1/./////////key$%#2"));
  }

  @Test
  public void testConstructFullPath() throws IOException {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setKeyName(KEY_TWO)
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(KEY_TWO_OBJECT_ID)
        .build();
    String fullPath = ReconUtils.constructFullPath(keyInfo,
        reconNamespaceSummaryManager);
    String expectedPath = "vol/bucket1/" + KEY_TWO;
    Assertions.assertEquals(expectedPath, fullPath);

    keyInfo = new OmKeyInfo.Builder()
        .setKeyName(KEY_FIVE)
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(KEY_FIVE_OBJECT_ID)
        .build();
    fullPath = ReconUtils.constructFullPath(keyInfo,
        reconNamespaceSummaryManager);
    expectedPath = "vol/bucket2/" + KEY_FIVE;
    Assertions.assertEquals(expectedPath, fullPath);

    keyInfo = new OmKeyInfo.Builder()
        .setKeyName(KEY_EIGHT)
        .setVolumeName(VOL_TWO)
        .setBucketName(BUCKET_THREE)
        .setObjectID(KEY_EIGHT_OBJECT_ID)
        .build();
    fullPath = ReconUtils.constructFullPath(keyInfo,
        reconNamespaceSummaryManager);
    expectedPath = "vol2/bucket3/" + KEY_EIGHT;
    Assertions.assertEquals(expectedPath, fullPath);


    keyInfo = new OmKeyInfo.Builder()
        .setKeyName(KEY_ELEVEN)
        .setVolumeName(VOL_TWO)
        .setBucketName(BUCKET_FOUR)
        .setObjectID(KEY_ELEVEN_OBJECT_ID)
        .build();
    fullPath = ReconUtils.constructFullPath(keyInfo,
        reconNamespaceSummaryManager);
    expectedPath = "vol2/bucket4/" + KEY_ELEVEN;
    Assertions.assertEquals(expectedPath, fullPath);
  }


  /**
   * Testing the following case.
   * └── vol
   *     ├── bucket1 (OBS)
   *     │   ├── file1
   *     │   ├── file2
   *     │   └── file3
   *     └── bucket2 (OBS)
   *         ├── file4
   *         └── file5
   * └── vol2
   *     ├── bucket3 (Legacy)
   *     │   ├── file8
   *     │   ├── file9
   *     │   └── file10
   *     └── bucket4 (Legacy)
   *         └── file11
   *
   * Write these keys to OM and
   * replicate them.
   * @throws Exception
   */
  @SuppressWarnings("checkstyle:MethodLength")
  private void populateOMDB() throws Exception {

    // write all keys
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        KEY_ONE,
        KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        FILE_ONE_SIZE,
        getOBSBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_ONE,
        VOL,
        KEY_TWO,
        KEY_TWO_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        FILE_TWO_SIZE,
        getOBSBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        KEY_THREE,
        KEY_THREE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        FILE_THREE_SIZE,
        getOBSBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        KEY_FOUR,
        KEY_FOUR_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        FILE_FOUR_SIZE,
        getOBSBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_FIVE,
        BUCKET_TWO,
        VOL,
        KEY_FIVE,
        KEY_FIVE_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        FILE_FIVE_SIZE,
        getOBSBucketLayout());

    writeKeyToOm(reconOMMetadataManager,
        KEY_EIGHT,
        BUCKET_THREE,
        VOL_TWO,
        KEY_EIGHT,
        KEY_EIGHT_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        FILE_EIGHT_SIZE,
        getLegacyBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_NINE,
        BUCKET_THREE,
        VOL_TWO,
        KEY_NINE,
        KEY_NINE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        FILE_NINE_SIZE,
        getLegacyBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_TEN,
        BUCKET_THREE,
        VOL_TWO,
        KEY_TEN,
        KEY_TEN_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        FILE_TEN_SIZE,
        getLegacyBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_ELEVEN,
        BUCKET_FOUR,
        VOL_TWO,
        KEY_ELEVEN,
        KEY_ELEVEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_FOUR_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        FILE_ELEVEN_SIZE,
        getLegacyBucketLayout());
  }

  /**
   * Create a new OM Metadata manager instance with one user, one vol, and two
   * buckets.
   *
   * @throws IOException ioEx
   */
  private static OMMetadataManager initializeNewOmMetadataManager(
      File omDbDir, OzoneConfiguration omConfiguration)
      throws IOException {
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    omConfiguration.set(OMConfigKeys
        .OZONE_OM_ENABLE_FILESYSTEM_PATHS, "false");
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

    omMetadataManager.getVolumeTable().put(volumeKey, args);
    omMetadataManager.getVolumeTable().put(volume2Key, args2);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(BUCKET_ONE_OBJECT_ID)
        .setQuotaInBytes(BUCKET_ONE_QUOTA)
        .setBucketLayout(getOBSBucketLayout())
        .build();

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(BUCKET_TWO_OBJECT_ID)
        .setQuotaInBytes(BUCKET_TWO_QUOTA)
        .setBucketLayout(getOBSBucketLayout())
        .build();

    OmBucketInfo bucketInfo3 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL_TWO)
        .setBucketName(BUCKET_THREE)
        .setObjectID(BUCKET_THREE_OBJECT_ID)
        .setQuotaInBytes(BUCKET_THREE_QUOTA)
        .setBucketLayout(getLegacyBucketLayout())
        .build();

    OmBucketInfo bucketInfo4 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL_TWO)
        .setBucketName(BUCKET_FOUR)
        .setObjectID(BUCKET_FOUR_OBJECT_ID)
        .setQuotaInBytes(BUCKET_FOUR_QUOTA)
        .setBucketLayout(getLegacyBucketLayout())
        .build();

    String bucketKey = omMetadataManager.getBucketKey(
        bucketInfo.getVolumeName(), bucketInfo.getBucketName());
    String bucketKey2 = omMetadataManager.getBucketKey(
        bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());
    String bucketKey3 = omMetadataManager.getBucketKey(
        bucketInfo3.getVolumeName(), bucketInfo3.getBucketName());
    String bucketKey4 = omMetadataManager.getBucketKey(
        bucketInfo4.getVolumeName(), bucketInfo4.getBucketName());

    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo);
    omMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);
    omMetadataManager.getBucketTable().put(bucketKey3, bucketInfo3);
    omMetadataManager.getBucketTable().put(bucketKey4, bucketInfo4);

    return omMetadataManager;
  }

  private void setUpMultiBlockKey() throws IOException {
    OmKeyLocationInfoGroup locationInfoGroup =
        getLocationInfoGroup1();

    // add the multi-block key to Recon's OM
    writeKeyToOm(reconOMMetadataManager,
        MULTI_BLOCK_FILE,
        BUCKET_ONE,
        VOL,
        MULTI_BLOCK_FILE,
        MULTI_BLOCK_KEY_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup),
        getOBSBucketLayout(),
        FILE_THREE_SIZE);
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
        KEY_ONE,
        KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getOBSBucketLayout(),
        FILE_ONE_SIZE);

    //vol/bucket1/file2
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_ONE,
        VOL,
        KEY_TWO,
        KEY_TWO_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getOBSBucketLayout(),
        FILE_TWO_SIZE);

    //vol/bucket1/file3
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        KEY_THREE,
        KEY_THREE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getOBSBucketLayout(),
        FILE_THREE_SIZE);

    //vol/bucket2/file4
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        KEY_FOUR,
        KEY_FOUR_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getOBSBucketLayout(),
        FILE_FOUR_SIZE);

    //vol/bucket2/file5
    writeKeyToOm(reconOMMetadataManager,
        KEY_FIVE,
        BUCKET_TWO,
        VOL,
        KEY_FIVE,
        KEY_FIVE_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getOBSBucketLayout(),
        FILE_FIVE_SIZE);

    //vol2/bucket3/file8
    writeKeyToOm(reconOMMetadataManager,
        KEY_EIGHT,
        BUCKET_THREE,
        VOL_TWO,
        KEY_EIGHT,
        KEY_EIGHT_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getLegacyBucketLayout(),
        FILE_EIGHT_SIZE);

    //vol2/bucket3/file9
    writeKeyToOm(reconOMMetadataManager,
        KEY_NINE,
        BUCKET_THREE,
        VOL_TWO,
        KEY_NINE,
        KEY_NINE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getLegacyBucketLayout(),
        FILE_NINE_SIZE);

    //vol2/bucket3/file10
    writeKeyToOm(reconOMMetadataManager,
        KEY_TEN,
        BUCKET_THREE,
        VOL_TWO,
        KEY_TEN,
        KEY_TEN_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        BUCKET_THREE_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup2),
        getLegacyBucketLayout(),
        FILE_TEN_SIZE);

    //vol2/bucket4/file11
    writeKeyToOm(reconOMMetadataManager,
        KEY_ELEVEN,
        BUCKET_FOUR,
        VOL_TWO,
        KEY_ELEVEN,
        KEY_ELEVEN_OBJECT_ID,
        BUCKET_FOUR_OBJECT_ID,
        BUCKET_FOUR_OBJECT_ID,
        VOL_TWO_OBJECT_ID,
        Collections.singletonList(locationInfoGroup1),
        getLegacyBucketLayout(),
        FILE_ELEVEN_SIZE);
  }

  /**
   * Generate a set of mock container replica with a size of
   * replication factor for container.
   *
   * @param replicationFactor number of replica
   * @param containerID       the container replicated based upon
   * @return a set of container replica for testing
   */
  private static Set<ContainerReplica> generateMockContainerReplicas(
      int replicationFactor, ContainerID containerID) {
    Set<ContainerReplica> result = new HashSet<>();
    for (int i = 0; i < replicationFactor; ++i) {
      DatanodeDetails randomDatanode = randomDatanodeDetails();
      ContainerReplica replica = new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(containerID)
          .setContainerState(
              StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN)
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

  private static BucketLayout getOBSBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }

  private static BucketLayout getLegacyBucketLayout() {
    return BucketLayout.LEGACY;
  }

  private static SCMNodeStat getMockSCMRootStat() {
    return new SCMNodeStat(ROOT_QUOTA, ROOT_DATA_SIZE,
        ROOT_QUOTA - ROOT_DATA_SIZE, 0L, 0L, 0);
  }

}
