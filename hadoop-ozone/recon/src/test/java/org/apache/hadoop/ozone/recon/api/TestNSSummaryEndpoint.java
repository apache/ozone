package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.BasicResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.VOLUME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProviderWithFSO;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeBucketToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeVolumeToOm;
import static org.mockito.Mockito.mock;

/**
 * Test for NSSummary REST APIs.
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

  // data size in bytes
  private static final long KEY_ONE_SIZE = 500L; // bin 0
  private static final long KEY_TWO_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_THREE_SIZE = 4 * OzoneConsts.KB + 1; // bin 3
  private static final long KEY_FOUR_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_FIVE_SIZE = 100L; // bin 0
  private static final long KEY_SIX_SIZE = 2 * OzoneConsts.KB + 1; // bin 2

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

  private boolean isSetUp = false;

  @Before
  public void setUp() throws Exception {
    // initial setting: "sampleVol", "bucketOne", "TestUser"
    if (isSetUp) {
      return;
    }
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
                    .addBinding(NSSummaryEndpoint.class)
                    .build();
    reconNamespaceSummaryManager =
            reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    nsSummaryEndpoint = reconTestInjector.getInstance(NSSummaryEndpoint.class);
    // enable FSO mode
    OzoneManagerRatisUtils.setBucketFSOptimized(true);

    // populate OM DB and reprocess into Recon RocksDB
    populateOMDB();
    NSSummaryTask nsSummaryTask = new NSSummaryTask(reconNamespaceSummaryManager);
    nsSummaryTask.reprocess(reconOMMetadataManager);
    isSetUp = true;
  }

  @Test
  public void testUtility() {
    String[] names = NSSummaryEndpoint.parseRequestPath(TEST_PATH_UTILITY);
    Assert.assertArrayEquals(TEST_NAMES, names);
    String keyName = NSSummaryEndpoint.getKeyName(names);
    Assert.assertEquals(TEST_KEY_NAMES, keyName);
  }

  @Test
  public void testBasic() throws Exception {
    // Test volume basics
    Response volResponse = nsSummaryEndpoint.getBasicInfo(VOL_PATH);
    BasicResponse volResponseObj = (BasicResponse) volResponse.getEntity();
    Assert.assertEquals(EntityType.VOLUME, volResponseObj.getEntityType());
    Assert.assertEquals(2, volResponseObj.getTotalBucket());
    Assert.assertEquals(4, volResponseObj.getTotalDir());
    Assert.assertEquals(6, volResponseObj.getTotalKey());

    // Test bucket 1's basics
    Response bucketOneResponse =
            nsSummaryEndpoint.getBasicInfo(BUCKET_ONE_PATH);
    BasicResponse bucketOneObj = (BasicResponse) bucketOneResponse.getEntity();
    Assert.assertEquals(EntityType.BUCKET, bucketOneObj.getEntityType());
    Assert.assertEquals(4, bucketOneObj.getTotalDir());
    Assert.assertEquals(4, bucketOneObj.getTotalKey());

    // Test bucket 2's basics
    Response bucketTwoResponse =
            nsSummaryEndpoint.getBasicInfo(BUCKET_TWO_PATH);
    BasicResponse bucketTwoObj = (BasicResponse) bucketTwoResponse.getEntity();
    Assert.assertEquals(EntityType.BUCKET, bucketTwoObj.getEntityType());
    Assert.assertEquals(0, bucketTwoObj.getTotalDir());
    Assert.assertEquals(2, bucketTwoObj.getTotalKey());

    // Test intermediate directory basics
    Response dirOneResponse = nsSummaryEndpoint.getBasicInfo(DIR_ONE_PATH);
    BasicResponse dirOneObj = (BasicResponse) dirOneResponse.getEntity();
    Assert.assertEquals(EntityType.DIRECTORY, dirOneObj.getEntityType());
    Assert.assertEquals(3, dirOneObj.getTotalDir());
    Assert.assertEquals(3, dirOneObj.getTotalKey());

    // Test invalid path
    Response invalidResponse = nsSummaryEndpoint.getBasicInfo(INVALID_PATH);
    BasicResponse invalidObj = (BasicResponse) invalidResponse.getEntity();
    Assert.assertTrue(invalidObj.isPathNotFound());

    // Test key
    Response keyResponse = nsSummaryEndpoint.getBasicInfo(KEY_PATH);
    BasicResponse keyResObj = (BasicResponse) keyResponse.getEntity();
    Assert.assertEquals(EntityType.KEY, keyResObj.getEntityType());
  }

  @Test
  public void testDiskUsage() throws Exception {
    // volume level DU
    Response volResponse = nsSummaryEndpoint.getDiskUsage(VOL_PATH);
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
    Response bucketResponse = nsSummaryEndpoint.getDiskUsage(BUCKET_ONE_PATH);
    DUResponse duBucketResponse = (DUResponse) bucketResponse.getEntity();
    Assert.assertEquals(1, duBucketResponse.getCount());
    DUResponse.DiskUsage duDir1 = duBucketResponse.getDuData().get(0);
    Assert.assertEquals(DIR_ONE_PATH, duDir1.getSubpath());
    Assert.assertEquals(DIR_ONE_DATA_SIZE, duDir1.getSize());

    // dir level DU
    Response dirResponse = nsSummaryEndpoint.getDiskUsage(DIR_ONE_PATH);
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
    Response keyResponse = nsSummaryEndpoint.getDiskUsage(KEY_PATH);
    DUResponse keyObj = (DUResponse) keyResponse.getEntity();
    Assert.assertEquals(1, keyObj.getCount());
    Assert.assertEquals(KEY_FOUR_SIZE, keyObj.getDuData().get(0).getSize());

    // invalid path check
    Response invalidResponse = nsSummaryEndpoint.getDiskUsage(INVALID_PATH);
    DUResponse invalidObj = (DUResponse) invalidResponse.getEntity();
    Assert.assertTrue(invalidObj.isPathNotFound());
  }

  @Test
  public void testQuotaUsage() throws Exception {

  }

  @Test
  public void testFileSizeDist() throws Exception {

  }

  /**
   * Initial configs:
   *                vol
   *             /       \
   *        bucket1      bucket2
   *        /    \         /    \
   *     file1    dir1    file4  file5
   *           /   \   \
   *        dir2  dir3  dir4
   *         /     \      \
   *       file2   file3  file6
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
   * buckets
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
}
