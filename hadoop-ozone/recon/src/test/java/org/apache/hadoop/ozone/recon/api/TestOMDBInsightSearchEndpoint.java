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
import org.glassfish.jersey.internal.Errors;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.*;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.mockito.Mockito.when;


/**
 * Test for OMDBInsightSearchEndpoint.
 */
public class TestOMDBInsightSearchEndpoint extends AbstractReconSqlDBTest {

  @TempDir
  private Path temporaryFolder;

  Logger LOG = LoggerFactory.getLogger(TestOMDBInsightSearchEndpoint.class);

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
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        getMockOzoneManagerServiceProviderWithFSO();
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
  public void testRootLevelSearch() throws IOException {
    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys(ROOT_PATH, true, true, 20);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(10, result.getFsoKeyInfoList().size());
    assertEquals(5, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(15000, result.getUnreplicatedDataSize());
    assertEquals(15000 * 3, result.getReplicatedDataSize());

    // Switch of the include Fso flag
    response =
        omdbInsightSearchEndpoint.searchOpenKeys(ROOT_PATH, false, true, 20);
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(0, result.getFsoKeyInfoList().size());
    assertEquals(5, result.getNonFSOKeyInfoList().size());

    // Switch of the include Non Fso flag
    response =
        omdbInsightSearchEndpoint.searchOpenKeys(ROOT_PATH, true, false, 20);
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(10, result.getFsoKeyInfoList().size());
  }

  @Test
  public void testBucketLevelSearch() throws IOException {
    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys("/volA/bucketA1", true, true, 20);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(5000, result.getUnreplicatedDataSize());
    assertEquals(5000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightSearchEndpoint.searchOpenKeys("/volB/bucketB1", true, true, 20);
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
        omdbInsightSearchEndpoint.searchOpenKeys("/volA/bucketA1/dirA1", true, true, 20);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightSearchEndpoint.searchOpenKeys("/volA/bucketA1/dirA2", true, true, 20);
    assertEquals(200, response.getStatus());
    result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightSearchEndpoint.searchOpenKeys("/volA/bucketA1/dirA3", true, true, 20);
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
        omdbInsightSearchEndpoint.searchOpenKeys(ROOT_PATH, true, true, 5);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getFsoKeyInfoList().size());
    assertEquals(5, result.getNonFSOKeyInfoList().size());
  }

  @Test
  public void testSearchOpenKeysWithNoMatchFound() throws IOException {
    // Given a search prefix that matches no keys
    String searchPrefix = "nonexistentKeyPrefix";

    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefix, true, true, 10);

    // Then the response should indicate that no keys were found
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        response.getStatus(), "Expected a 404 NOT FOUND status");

    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys exist for the specified search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testSearchOpenKeysWithBadRequest() throws IOException {
    // Given a search prefix that is empty
    String searchPrefix = "";

    Response response =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefix, true, true, 10);

    // Then the response should indicate that the request was bad
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        response.getStatus(), "Expected a 400 BAD REQUEST status");

    String entity = (String) response.getEntity();
    assertTrue(entity.contains("The searchPrefix query parameter is required."),
        "Expected a message indicating the search prefix cannot be empty");
  }


  /**
   * Tests the NSSummaryEndpoint for a given volume, bucket, and directory structure.
   * The test setup mimics the following filesystem structure with specified sizes:
   *
   * root   (Total Size: 15000KB)
   * ├── volA   (Total Size: 10000KB)
   * │   ├── bucketA1   (FSO) Total Size: 5000KB
   * │   │   ├── fileA1 (Size: 1000KB)
   * │   │   ├── fileA2 (Size: 1000KB)
   * │   │   ├── dirA1 (Total Size: 1000KB)
   * │   │   ├── dirA2 (Total Size: 1000KB)
   * │   │   └── dirA3 (Total Size: 1000KB)
   * │   ├── bucketA2   (FSO) Total Size: 5000KB
   * │   │   ├── fileA3 (Size: 1000KB)
   * │   │   ├── fileA4 (Size: 1000KB)
   * │   │   ├── dirA4 (Total Size: 1000KB)
   * │   │   ├── dirA5 (Total Size: 1000KB)
   * │   │   └── dirA6 (Total Size: 1000KB)
   * └── volB   (Total Size: 5000KB)
   *     └── bucketB1   (OBS) Total Size: 5000KB
   *         ├── fileB1 (Size: 1000KB)
   *         ├── fileB2 (Size: 1000KB)
   *         ├── fileB3 (Size: 1000KB)
   *         ├── fileB4 (Size: 1000KB)
   *         └── fileB5 (Size: 1000KB)
   *
   * @throws Exception
   */
  private void populateOMDB() throws Exception {
    // Create Volumes
    long volAObjectId = createVolume("volA");
    long volBObjectId = createVolume("volB");

    // Create Buckets in volA
    long bucketA1ObjectId =
        createBucket("volA", "bucketA1", 1000 + 1000 + 1000 + 1000 + 1000,
            getFSOBucketLayout());
    long bucketA2ObjectId =
        createBucket("volA", "bucketA2", 1000 + 1000 + 1000 + 1000 + 1000,
            getFSOBucketLayout());

    // Create Bucket in volB
    long bucketB1ObjectId =
        createBucket("volB", "bucketB1", 1000 + 1000 + 1000 + 1000 + 1000,
            getOBSBucketLayout());

    // Create Directories and Files under bucketA1
    long dirA1ObjectId =
        createDirectory(bucketA1ObjectId, bucketA1ObjectId, volAObjectId,
            "dirA1");
    long dirA2ObjectId =
        createDirectory(bucketA1ObjectId, bucketA1ObjectId, volAObjectId,
            "dirA2");
    long dirA3ObjectId =
        createDirectory(bucketA1ObjectId, bucketA1ObjectId, volAObjectId,
            "dirA3");

    // Files directly under bucketA1
    createOpenFile("fileA1", "bucketA1", "volA", "fileA1", bucketA1ObjectId,
        bucketA1ObjectId, volAObjectId, 1000);
    createOpenFile("fileA2", "bucketA1", "volA", "fileA2", bucketA1ObjectId,
        bucketA1ObjectId, volAObjectId, 1000);

    // Create Directories and Files under bucketA2
    long dirA4ObjectId =
        createDirectory(bucketA2ObjectId, bucketA2ObjectId, volAObjectId,
            "dirA4");
    long dirA5ObjectId =
        createDirectory(bucketA2ObjectId, bucketA2ObjectId, volAObjectId,
            "dirA5");
    long dirA6ObjectId =
        createDirectory(bucketA2ObjectId, bucketA2ObjectId, volAObjectId,
            "dirA6");

    // Files directly under bucketA2
    createOpenFile("fileA3", "bucketA2", "volA", "fileA3", bucketA2ObjectId,
        bucketA2ObjectId, volAObjectId, 1000);
    createOpenFile("fileA4", "bucketA2", "volA", "fileA4", bucketA2ObjectId,
        bucketA2ObjectId, volAObjectId, 1000);

    // Files directly under bucketB1
    createOpenKey("fileB1", "bucketB1", "volB", "fileB1", bucketB1ObjectId,
        bucketB1ObjectId, volBObjectId, 1000);
    createOpenKey("fileB2", "bucketB1", "volB", "fileB2", bucketB1ObjectId,
        bucketB1ObjectId, volBObjectId, 1000);
    createOpenKey("fileB3", "bucketB1", "volB", "fileB3", bucketB1ObjectId,
        bucketB1ObjectId, volBObjectId, 1000);
    createOpenKey("fileB4", "bucketB1", "volB", "fileB4", bucketB1ObjectId,
        bucketB1ObjectId, volBObjectId, 1000);
    createOpenKey("fileB5", "bucketB1", "volB", "fileB5", bucketB1ObjectId,
        bucketB1ObjectId, volBObjectId, 1000);

    // Create Inner files under directories
    createOpenFile("dirA1/innerFile", "bucketA1", "volA", "innerFile",
        dirA1ObjectId, bucketA1ObjectId, volAObjectId, 1000);
    createOpenFile("dirA2/innerFile", "bucketA1", "volA", "innerFile",
        dirA2ObjectId, bucketA1ObjectId, volAObjectId, 1000);
    createOpenFile("dirA3/innerFile", "bucketA1", "volA", "innerFile",
        dirA3ObjectId, bucketA1ObjectId, volAObjectId, 1000);
    createOpenFile("dirA4/innerFile", "bucketA2", "volA", "innerFile",
        dirA4ObjectId, bucketA2ObjectId, volAObjectId, 1000);
    createOpenFile("dirA5/innerFile", "bucketA2", "volA", "innerFile",
        dirA5ObjectId, bucketA2ObjectId, volAObjectId, 1000);
    createOpenFile("dirA6/innerFile", "bucketA2", "volA", "innerFile",
        dirA6ObjectId, bucketA2ObjectId, volAObjectId, 1000);
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
                             String fileName,
                             long parentObjectId,
                             long bucketObjectId,
                             long volumeObjectId,
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
