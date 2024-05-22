/**
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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;

import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithFSO;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenFileToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenKeyToOm;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Test for OMDBInsightSearchEndpoint.
 */
public class TestOMDBInsightSearchEndpoint extends AbstractReconSqlDBTest {

  @TempDir
  private Path temporaryFolder;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OMDBInsightSearchEndpoint omdbInsightSearchEndpoint;
  private OzoneConfiguration ozoneConfiguration;
  private static final String ROOT_PATH = "/";
  private static final String TEST_USER = "TestUser";
  private OMMetadataManager omMetadataManager;

  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @BeforeEach
  public void setUp() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setLong(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD,
        100);
    omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve("JunitOmDBDir"))
            .toFile());
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(temporaryFolder.resolve("OmMetataDir")).toFile());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(OMDBInsightEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .build();
    reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    omdbInsightSearchEndpoint = reconTestInjector.getInstance(
        OMDBInsightSearchEndpoint.class);

    // populate OM DB and reprocess into Recon RocksDB
    populateOMDB();
    NSSummaryTaskWithFSO nSSummaryTaskWithFso =
        new NSSummaryTaskWithFSO(reconNamespaceSummaryManager,
            reconOMMetadataManager, ozoneConfiguration);
    nSSummaryTaskWithFso.reprocessWithFSO(reconOMMetadataManager);
  }

  /**
   * Create a new OM Metadata manager instance with one user, one vol, and two
   * buckets.
   *
   * @throws IOException ioEx
   */
  private static OMMetadataManager initializeNewOmMetadataManager(
      File omDbDir)
      throws IOException {
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
        omConfiguration, null);
    return omMetadataManager;
  }

  @Test
  public void testVolumeLevelSearchRestriction() throws IOException {
    // Test with volume level path
    String volumePath = "/vola";
    Response response = omdbInsightSearchEndpoint.searchOpenKeys(volumePath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    // Test with another volume level path
    volumePath = "/volb";
    response = omdbInsightSearchEndpoint.searchOpenKeys(volumePath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testRootLevelSearchRestriction() throws IOException {
    // Test with root level path
    String rootPath = "/";
    Response response = omdbInsightSearchEndpoint.searchOpenKeys(rootPath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    // Test with root level path without trailing slash
    rootPath = "";
    response = omdbInsightSearchEndpoint.searchOpenKeys(rootPath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testBucketLevelSearch() throws IOException {
    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys("/vola/bucketa1", 20, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(5000, result.getUnreplicatedDataSize());
    assertEquals(5000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightSearchEndpoint.searchOpenKeys("/volb/bucketb1", 20, "");
    assertEquals(200, response.getStatus());
    result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());
    // Assert Total Size
    assertEquals(5000, result.getUnreplicatedDataSize());
    assertEquals(5000 * 3, result.getReplicatedDataSize());
  }

  @Test
  public void testDirectoryLevelSearch() throws IOException {
    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys("/vola/bucketa1/dira1", 20, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightSearchEndpoint.searchOpenKeys("/vola/bucketa1/dira2", 20, "");
    assertEquals(200, response.getStatus());
    result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightSearchEndpoint.searchOpenKeys("/vola/bucketa1/dira3", 20, "");
    assertEquals(200, response.getStatus());
    result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());
  }

  @Test
  public void testLimitSearch() throws IOException {
    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys("/vola/bucketa1", 2, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
  }

  @Test
  public void testSearchOpenKeysWithNoMatchFound() throws IOException {
    // Given a search prefix that matches no keys
    String searchPrefix = "unknown-volume/unknown-bucket/";

    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefix, 10, "");

    // Then the response should indicate that no keys were found
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        response.getStatus(), "Expected a 404 NOT FOUND status");

    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testSearchOpenKeysWithBadRequest() throws IOException {
    // Give a negative limit
    int negativeLimit = -1;
    Response response = omdbInsightSearchEndpoint.searchOpenKeys("@323232", negativeLimit, "");

    // Then the response should indicate that the request was bad
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        response.getStatus(), "Expected a 400 BAD REQUEST status");

    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testLastKeyInResponse() throws IOException {
    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys("/volb/bucketb1", 20, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(0, result.getFsoKeyInfoList().size());
    assertEquals(5, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(5000, result.getUnreplicatedDataSize());
    assertEquals(5000 * 3, result.getReplicatedDataSize());
    // Assert Last Key
    assertEquals(ROOT_PATH + "volb/bucketb1/fileb5", result.getLastKey(),
        "Expected last key to be 'fileb5'");
  }

  @Test
  public void testSearchOpenKeysWithPagination() throws IOException {
    // Set the initial parameters
    String startPrefix = "/volb/bucketb1";
    int limit = 2;
    String prevKey = "";

    // Perform the first search request
    Response response = omdbInsightSearchEndpoint.searchOpenKeys(startPrefix, limit, prevKey);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());

    // Extract the last key from the response
    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    // Perform the second search request using the last key
    response = omdbInsightSearchEndpoint.searchOpenKeys(startPrefix, limit, prevKey);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());

    // Extract the last key from the response
    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    // Perform the third search request using the last key
    response = omdbInsightSearchEndpoint.searchOpenKeys(startPrefix, limit, prevKey);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());
  }


  /**
   * Tests the NSSummaryEndpoint for a given volume, bucket, and directory structure.
   * The test setup mimics the following filesystem structure with specified sizes:
   *
   * root   (Total Size: 15000KB)
   * ├── vola   (Total Size: 10000KB)
   * │   ├── bucketa1   (FSO) Total Size: 5000KB
   * │   │   ├── filea1 (Size: 1000KB)
   * │   │   ├── filea2 (Size: 1000KB)
   * │   │   ├── dira1 (Total Size: 1000KB)
   * │   │   ├── dira2 (Total Size: 1000KB)
   * │   │   └── dira3 (Total Size: 1000KB)
   * │   ├── bucketa2   (FSO) Total Size: 5000KB
   * │   │   ├── filea3 (Size: 1000KB)
   * │   │   ├── filea4 (Size: 1000KB)
   * │   │   ├── dira4 (Total Size: 1000KB)
   * │   │   ├── dira5 (Total Size: 1000KB)
   * │   │   └── dira6 (Total Size: 1000KB)
   * └── volb   (Total Size: 5000KB)
   *     └── bucketb1   (OBS) Total Size: 5000KB
   *         ├── fileb1 (Size: 1000KB)
   *         ├── fileb2 (Size: 1000KB)
   *         ├── fileb3 (Size: 1000KB)
   *         ├── fileb4 (Size: 1000KB)
   *         └── fileb5 (Size: 1000KB)
   *
   * @throws Exception
   */
  private void populateOMDB() throws Exception {
    // Create Volumes
    long volaObjectId = createVolume("vola");
    createVolume("volb");

    // Create Buckets in vola
    long bucketa1ObjectId =
        createBucket("vola", "bucketa1", 1000 + 1000 + 1000 + 1000 + 1000,
            getFSOBucketLayout());
    long bucketa2ObjectId =
        createBucket("vola", "bucketa2", 1000 + 1000 + 1000 + 1000 + 1000,
            getFSOBucketLayout());

    // Create Bucket in volb
    createBucket("volb", "bucketb1", 1000 + 1000 + 1000 + 1000 + 1000,
            getOBSBucketLayout());

    // Create Directories and Files under bucketa1
    long dira1ObjectId =
        createDirectory(bucketa1ObjectId, bucketa1ObjectId, volaObjectId,
            "dira1");
    long dira2ObjectId =
        createDirectory(bucketa1ObjectId, bucketa1ObjectId, volaObjectId,
            "dira2");
    long dira3ObjectId =
        createDirectory(bucketa1ObjectId, bucketa1ObjectId, volaObjectId,
            "dira3");

    // Files directly under bucketa1
    createOpenFile("filea1", "bucketa1", "vola", "filea1", bucketa1ObjectId,
        bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("filea2", "bucketa1", "vola", "filea2", bucketa1ObjectId,
        bucketa1ObjectId, volaObjectId, 1000);

    // Create Directories and Files under bucketa2
    long dira4ObjectId =
        createDirectory(bucketa2ObjectId, bucketa2ObjectId, volaObjectId,
            "dira4");
    long dira5ObjectId =
        createDirectory(bucketa2ObjectId, bucketa2ObjectId, volaObjectId,
            "dira5");
    long dira6ObjectId =
        createDirectory(bucketa2ObjectId, bucketa2ObjectId, volaObjectId,
            "dira6");

    // Files directly under bucketa2
    createOpenFile("filea3", "bucketa2", "vola", "filea3", bucketa2ObjectId,
        bucketa2ObjectId, volaObjectId, 1000);
    createOpenFile("filea4", "bucketa2", "vola", "filea4", bucketa2ObjectId,
        bucketa2ObjectId, volaObjectId, 1000);

    // Files directly under bucketb1
    createOpenKey("fileb1", "bucketb1", "volb", 1000);
    createOpenKey("fileb2", "bucketb1", "volb", 1000);
    createOpenKey("fileb3", "bucketb1", "volb", 1000);
    createOpenKey("fileb4", "bucketb1", "volb", 1000);
    createOpenKey("fileb5", "bucketb1", "volb", 1000);

    // Create Inner files under directories
    createOpenFile("dira1/innerfile", "bucketa1", "vola", "innerfile",
        dira1ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira2/innerfile", "bucketa1", "vola", "innerfile",
        dira2ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira3/innerfile", "bucketa1", "vola", "innerfile",
        dira3ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira4/innerfile", "bucketa2", "vola", "innerfile",
        dira4ObjectId, bucketa2ObjectId, volaObjectId, 1000);
    createOpenFile("dira5/innerfile", "bucketa2", "vola", "innerfile",
        dira5ObjectId, bucketa2ObjectId, volaObjectId, 1000);
    createOpenFile("dira6/innerfile", "bucketa2", "vola", "innerfile",
        dira6ObjectId, bucketa2ObjectId, volaObjectId, 1000);
  }

  /**
   * Create a volume and add it to the Volume Table.
   *
   * @return volume Object ID
   * @throws IOException
   */
  private long createVolume(String volumeName) throws Exception {
    String volumeKey = reconOMMetadataManager.getVolumeKey(volumeName);
    long volumeId = UUID.randomUUID().getMostSignificantBits() &
        Long.MAX_VALUE; // Generate positive ID
    OmVolumeArgs args = OmVolumeArgs.newBuilder()
        .setObjectID(volumeId)
        .setVolume(volumeName)
        .setAdminName(TEST_USER)
        .setOwnerName(TEST_USER)
        .build();

    reconOMMetadataManager.getVolumeTable().put(volumeKey, args);
    return volumeId;
  }

  /**
   * Create a bucket and add it to the Bucket Table.
   *
   * @return bucket Object ID
   * @throws IOException
   */
  private long createBucket(String volumeName, String bucketName, long dataSize,
                            BucketLayout bucketLayout)
      throws Exception {
    String bucketKey =
        reconOMMetadataManager.getBucketKey(volumeName, bucketName);
    long bucketId = UUID.randomUUID().getMostSignificantBits() &
        Long.MAX_VALUE; // Generate positive ID
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setObjectID(bucketId)
        .setBucketLayout(bucketLayout)
        .setUsedBytes(dataSize)
        .build();

    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo);
    return bucketId;
  }

  /**
   * Create a directory and add it to the Directory Table.
   *
   * @return directory Object ID
   * @throws IOException
   */
  private long createDirectory(long parentObjectId,
                               long bucketObjectId,
                               long volumeObjectId,
                               String dirName) throws IOException {
    long objectId = UUID.randomUUID().getMostSignificantBits() &
        Long.MAX_VALUE; // Ensure positive ID
    writeDirToOm(reconOMMetadataManager, objectId, parentObjectId,
        bucketObjectId,
        volumeObjectId, dirName);
    return objectId;
  }

  /**
   * Create a file and add it to the Open File Table.
   *
   * @return file Object ID
   * @throws IOException
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private long createOpenFile(String key,
                              String bucket,
                              String volume,
                              String fileName,
                              long parentObjectId,
                              long bucketObjectId,
                              long volumeObjectId,
                              long dataSize) throws IOException {
    long objectId = UUID.randomUUID().getMostSignificantBits() &
        Long.MAX_VALUE; // Ensure positive ID
    writeOpenFileToOm(reconOMMetadataManager, key, bucket, volume, fileName,
        objectId, parentObjectId, bucketObjectId, volumeObjectId, null,
        dataSize);
    return objectId;
  }

  /**
   * Create a key and add it to the Open Key Table.
   *
   * @return key Object ID
   * @throws IOException
   */
  private long createOpenKey(String key,
                             String bucket,
                             String volume,
                             long dataSize) throws IOException {
    long objectId = UUID.randomUUID().getMostSignificantBits() &
        Long.MAX_VALUE; // Ensure positive ID
    writeOpenKeyToOm(reconOMMetadataManager, key, bucket, volume, null,
        dataSize);
    return objectId;
  }

  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  private static BucketLayout getOBSBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }

}
