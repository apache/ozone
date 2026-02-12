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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenFileToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeOpenKeyToOm;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithFSO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for OMDBInsightSearchEndpoint.
 *
 * This class tests various scenarios for searching open keys within a
 * given volume, bucket, and directory structure. The tests include:
 *
 * 1. Test Root Level Search Restriction: Ensures searching at the root level returns a bad request.
 * 2. Test Volume Level Search Restriction: Ensures searching at the volume level returns a bad request.
 * 3. Test Bucket Level Search: Verifies search results within different types of buckets (FSO, OBS, Legacy).
 * 4. Test Directory Level Search: Validates searching inside specific directories.
 * 5. Test Key Level Search: Confirms search results for specific keys within buckets.
 * 6. Test Key Level Search Under Directory: Verifies searching for keys within nested directories.
 * 7. Test Search Under Nested Directory: Checks search results within nested directories under dira3.
 * 8. Test Limit Search: Tests the limit functionality of the search API.
 * 9. Test Search Open Keys with Bad Request: Ensures bad requests with invalid parameters return appropriate responses.
 * 10. Test Last Key in Response: Confirms the presence of the last key in paginated responses.
 * 11. Test Search Open Keys with Pagination: Verifies paginated search results.
 * 12. Test Search in Empty Bucket: Checks the response for searching within an empty bucket.
 */
public class TestOpenKeysSearchEndpoint extends AbstractReconSqlDBTest {

  @TempDir
  private Path temporaryFolder;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OMDBInsightEndpoint omdbInsightEndpoint;
  private static final String ROOT_PATH = "/";
  private static final String TEST_USER = "TestUser";

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setLong(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, 100);
    OMMetadataManager omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve("JunitOmDBDir")).toFile());
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
    ReconNamespaceSummaryManager reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    omdbInsightEndpoint = reconTestInjector.getInstance(OMDBInsightEndpoint.class);
    // populate OM DB and reprocess into Recon RocksDB
    populateOMDB();
    NSSummaryTaskWithFSO nSSummaryTaskWithFso =
        new NSSummaryTaskWithFSO(reconNamespaceSummaryManager,
            reconOMMetadataManager, 10, 5, 20, 2000);
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
  public void testRootLevelSearchRestriction() throws IOException {
    // Test with root level path
    String rootPath = "/";
    Response response =
        omdbInsightEndpoint.getOpenKeyInfo(-1, "", rootPath, true, true);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testVolumeLevelSearchRestriction() throws IOException {
    // Test with volume level path
    String volumePath = "/vola";
    Response response = omdbInsightEndpoint.getOpenKeyInfo(20, "", volumePath, true, true);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    // Test with another volume level path
    volumePath = "/volb";
    response = omdbInsightEndpoint.getOpenKeyInfo(20, "", volumePath, true, true);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testBucketLevelSearch() throws IOException {
    // Search inside FSO bucket
    Response response =
        omdbInsightEndpoint.getOpenKeyInfo(20, "", "/vola/bucketa1", true, true);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(14, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(14000, result.getUnreplicatedDataSize());
    assertEquals(14000 * 3, result.getReplicatedDataSize());

    // Search inside OBS bucket
    response =
        omdbInsightEndpoint.getOpenKeyInfo(20, "", "/volb/bucketb1", true, true);
    assertEquals(200, response.getStatus());
    result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());
    // Assert Total Size
    assertEquals(5000, result.getUnreplicatedDataSize());
    assertEquals(5000 * 3, result.getReplicatedDataSize());

    // Search Inside LEGACY bucket
    response =
        omdbInsightEndpoint.getOpenKeyInfo(20, "", "/volc/bucketc1", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(7, result.getNonFSOKeyInfoList().size());

    // Test with bucket that does not exist
    response = omdbInsightEndpoint.getOpenKeyInfo(20, "", "/vola/nonexistentbucket", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testDirectoryLevelSearch() throws IOException {
    Response response =
        omdbInsightEndpoint.getOpenKeyInfo(20, "", "/vola/bucketa1/dira1", true, true);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightEndpoint.getOpenKeyInfo(20, "", "/vola/bucketa1/dira2", true, true);
    assertEquals(200, response.getStatus());
    result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response =
        omdbInsightEndpoint.getOpenKeyInfo(20, "", "/vola/bucketa1/dira3", true, true);
    assertEquals(200, response.getStatus());
    result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(10, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(10000, result.getUnreplicatedDataSize());
    assertEquals(10000 * 3, result.getReplicatedDataSize());

    // Test with non-existent directory
    response = omdbInsightEndpoint.getOpenKeyInfo(20, "", "/vola/bucketa1/nonexistentdir", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testKeyLevelSearch() throws IOException {
    // FSO Bucket key-level search
    Response response = omdbInsightEndpoint.getOpenKeyInfo(10, "", "/vola/bucketa1/filea1", true, true);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response = omdbInsightEndpoint.getOpenKeyInfo(10, "", "/vola/bucketa1/filea2", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    // OBS Bucket key-level search
    response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/volb/bucketb1/fileb1", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(0, result.getFsoKeyInfoList().size());
    assertEquals(1, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/volb/bucketb1/fileb2", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(0, result.getFsoKeyInfoList().size());
    assertEquals(1, result.getNonFSOKeyInfoList().size());
    // Assert Total Size
    assertEquals(1000, result.getUnreplicatedDataSize());
    assertEquals(1000 * 3, result.getReplicatedDataSize());

    // Test with non-existent key
    response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/volb/bucketb1/nonexistentfile", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");

    response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/volb/bucketb1/nonexistentfile", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  // Test searching for keys under a directory
  @Test
  public void testKeyLevelSearchUnderDirectory() throws IOException {
    // FSO Bucket key-level search
    Response response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/vola/bucketa1/dira1/innerfile", true, true);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());

    response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/vola/bucketa1/dira2/innerfile", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());

    // Test for unknown file in fso bucket
    response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/vola/bucketa1/dira1/unknownfile", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");

    // Test for unknown file in fso bucket
    response = omdbInsightEndpoint
        .getOpenKeyInfo(10, "", "/vola/bucketa1/dira2/unknownfile", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testSearchUnderNestedDirectory() throws IOException {
    Response response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/vola/bucketa1/dira3", true, true);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(10, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());

    // Search under dira31
    response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/vola/bucketa1/dira3/dira31", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(6, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());

    // Search under dira32
    response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/vola/bucketa1/dira3/dira31/dira32", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(3, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());

    // Search under dira33
    response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/vola/bucketa1/dira3/dira31/dira32/dira33", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());

    // Search for the exact file under dira33
    response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/vola/bucketa1/dira3/dira31/dira32/dira33/file33_1", true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());

    // Search for a non existant file under each nested directory
    response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/vola/bucketa1/dira3/dira31/dira32/dira33/nonexistentfile", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");

    response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/vola/bucketa1/dira3/dira31/dira32/nonexistentfile", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testLimitSearch() throws IOException {
    Response response =
        omdbInsightEndpoint.getOpenKeyInfo(2, "", "/vola/bucketa1", true, true);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getFsoKeyInfoList().size());
    assertEquals(0, result.getNonFSOKeyInfoList().size());
  }

  @Test
  public void testSearchOpenKeysWithBadRequest() throws IOException {
    // Give a negative limit
    int negativeLimit = -1;
    Response response = omdbInsightEndpoint
        .getOpenKeyInfo(negativeLimit, "", "@323232", true, true);
    // Then the response should indicate that the request was bad
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        response.getStatus(), "Expected a 400 BAD REQUEST status");

    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    response = omdbInsightEndpoint.getOpenKeyInfo(20, "", "///", true, true);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testLastKeyInResponse() throws IOException {
    Response response = omdbInsightEndpoint
        .getOpenKeyInfo(20, "", "/volb/bucketb1", true, true);
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
    Response response = omdbInsightEndpoint.getOpenKeyInfo(limit, prevKey, startPrefix, true, true);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());

    // Extract the last key from the response
    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    // Perform the second search request using the last key
    response = omdbInsightEndpoint.getOpenKeyInfo(limit, prevKey, startPrefix, true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());

    // Extract the last key from the response
    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    // Perform the third search request using the last key
    response = omdbInsightEndpoint.getOpenKeyInfo(limit, prevKey, startPrefix, true, true);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getNonFSOKeyInfoList().size());
    assertEquals(0, result.getFsoKeyInfoList().size());
    assertEquals(result.getNonFSOKeyInfoList().get(0).getKey(), result.getLastKey(),
        "Expected last key to be empty");
  }

  @Test
  public void testSearchInEmptyBucket() throws IOException {
    // Search in empty bucket bucketb2
    Response response = omdbInsightEndpoint.getOpenKeyInfo(20, "", "/volb/bucketb2", true, true);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testSearchWithPrevKeyOnly() throws IOException {
    String prevKey = "/volb/bucketb1/fileb1";  // Key exists in volb/bucketb1
    Response response = omdbInsightEndpoint.getOpenKeyInfo(4, prevKey, "", true, true);

    assertEquals(200, response.getStatus());

    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(4, result.getNonFSOKeyInfoList().size(), "Expected 4 remaining keys after 'fileb1'");
    assertEquals("/volb/bucketb1/fileb5", result.getLastKey(), "Expected last key to be 'fileb5'");
  }

  @Test
  public void testSearchWithEmptyPrevKeyAndStartPrefix() throws IOException {
    Response response = omdbInsightEndpoint.getOpenKeyInfo(-1, "", "", true, true);

    assertEquals(200, response.getStatus());

    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    // Assert all the keys are returned
    assertEquals(12, result.getNonFSOKeyInfoList().size(), "Expected all keys to be returned");
  }

  @Test
  public void testSearchWithStartPrefixOnly() throws IOException {
    String startPrefix = "/volb/bucketb1/";
    Response response = omdbInsightEndpoint.getOpenKeyInfo(10, "", startPrefix, true, true);

    assertEquals(200, response.getStatus());

    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getNonFSOKeyInfoList().size(), "Expected 5 keys starting with 'fileb1'");
    assertEquals("/volb/bucketb1/fileb5", result.getLastKey(), "Expected last key to be 'fileb5'");
  }

  @Test
  public void testSearchWithPrevKeyAndStartPrefix() throws IOException {
    String startPrefix = "/volb/bucketb1/";
    String prevKey = "/volb/bucketb1/fileb1";
    Response response = omdbInsightEndpoint.getOpenKeyInfo(10, prevKey, startPrefix, true, true);

    assertEquals(200, response.getStatus());

    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(4, result.getNonFSOKeyInfoList().size(), "Expected 4 keys after 'fileb1'");
    assertEquals("/volb/bucketb1/fileb5", result.getLastKey(), "Expected last key to be 'fileb5'");
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
   * │   │       ├── dira31 (Total Size: 1000KB)
   * │   │       ├── dira32 (Total Size: 1000KB)
   * │   │       └── dira33 (Total Size: 1000KB)
   * │   ├── bucketa2   (FSO) Total Size: 5000KB
   * │   │   ├── filea3 (Size: 1000KB)
   * │   │   ├── filea4 (Size: 1000KB)
   * │   │   ├── dira4 (Total Size: 1000KB)
   * │   │   ├── dira5 (Total Size: 1000KB)
   * │   │   └── dira6 (Total Size: 1000KB)
   * └── volb   (Total Size: 5000KB)
   *     ├── bucketb1   (OBS) Total Size: 5000KB
   *     │   ├── fileb1 (Size: 1000KB)
   *     │   ├── fileb2 (Size: 1000KB)
   *     │   ├── fileb3 (Size: 1000KB)
   *     │   ├── fileb4 (Size: 1000KB)
   *     │   └── fileb5 (Size: 1000KB)
   *     └── bucketb2   (OBS) Total Size: 0KB (Empty Bucket)
   * └── volc   (Total Size: 7000KB)
   *     └── bucketc1   (LEGACY) Total Size: 7000KB
   *         ├── filec1 (Size: 1000KB)
   *         ├── filec2 (Size: 1000KB)
   *         ├── filec3 (Size: 1000KB)
   *         ├── dirc1/ (Total Size: 2000KB)
   *         └── dirc2/ (Total Size: 2000KB)
   *
   * @throws Exception
   */
  private void populateOMDB() throws Exception {
    // Create Volumes
    long volaObjectId = createVolume("vola");
    createVolume("volb");
    createVolume("volc");

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
    createBucket("volb", "bucketb2", 0, getOBSBucketLayout()); // Empty Bucket

    // Create Bucket in volc
    createBucket("volc", "bucketc1", 7000,
        getLegacyBucketLayout());

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

    // Create nested directories under dira3
    long dira31ObjectId =
        createDirectory(dira3ObjectId, bucketa1ObjectId, volaObjectId,
            "dira31");
    long dira32ObjectId =
        createDirectory(dira31ObjectId, bucketa1ObjectId, volaObjectId,
            "dira32");
    long dira33ObjectId =
        createDirectory(dira32ObjectId, bucketa1ObjectId, volaObjectId,
            "dira33");

    // Files directly under bucketa1
    createOpenFile("filea1", "bucketa1", "vola", "filea1", bucketa1ObjectId,
        bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("filea2", "bucketa1", "vola", "filea2", bucketa1ObjectId,
        bucketa1ObjectId, volaObjectId, 1000);

    // Files under dira3
    createOpenFile("dira3/file3_1", "bucketa1", "vola", "file3_1",
        dira3ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira3/file3_2", "bucketa1", "vola", "file3_2",
        dira3ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira3/file3_3", "bucketa1", "vola", "file3_3",
        dira3ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira3/file3_4", "bucketa1", "vola", "file3_4",
        dira3ObjectId, bucketa1ObjectId, volaObjectId, 1000);

    // Files under dira31
    createOpenFile("dira3/dira31/file31_1", "bucketa1", "vola", "file31_1",
        dira31ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira3/dira31/file31_2", "bucketa1", "vola", "file31_2",
        dira31ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira3/dira31/file31_3", "bucketa1", "vola", "file31_3",
        dira31ObjectId, bucketa1ObjectId, volaObjectId, 1000);

    // Files under dira32
    createOpenFile("dira3/dira31/dira32/file32_1", "bucketa1", "vola", "file32_1",
        dira32ObjectId, bucketa1ObjectId, volaObjectId, 1000);
    createOpenFile("dira3/dira31/dira32/file32_2", "bucketa1", "vola", "file32_2",
        dira32ObjectId, bucketa1ObjectId, volaObjectId, 1000);

    // Files under dira33
    createOpenFile("dira3/dira31/dira32/dira33/file33_1", "bucketa1", "vola", "file33_1",
        dira33ObjectId, bucketa1ObjectId, volaObjectId, 1000);

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
    createOpenFile("dira4/innerfile", "bucketa2", "vola", "innerfile",
        dira4ObjectId, bucketa2ObjectId, volaObjectId, 1000);
    createOpenFile("dira5/innerfile", "bucketa2", "vola", "innerfile",
        dira5ObjectId, bucketa2ObjectId, volaObjectId, 1000);
    createOpenFile("dira6/innerfile", "bucketa2", "vola", "innerfile",
        dira6ObjectId, bucketa2ObjectId, volaObjectId, 1000);

    // Create Keys and Directories in bucketc1 (LEGACY layout)
    createOpenKey("filec1", "bucketc1", "volc", 1000);
    createOpenKey("filec2", "bucketc1", "volc", 1000);
    createOpenKey("filec3", "bucketc1", "volc", 1000);
    createOpenKey("dirc1/", "bucketc1", "volc", 2000); // Directory indicated by trailing slash
    createOpenKey("dirc2/", "bucketc1", "volc", 2000); // Directory indicated by trailing slash
    createOpenKey("dirc1/innerfile", "bucketc1", "volc", 2000); // File in directory
    createOpenKey("dirc2/innerfile", "bucketc1", "volc", 2000); // File in directory
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

  private static BucketLayout getLegacyBucketLayout() {
    return BucketLayout.LEGACY;
  }

}
