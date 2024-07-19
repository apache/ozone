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

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test class for DeletedKeysSearchEndpoint.
 *
 * This class tests various scenarios for searching deleted keys within a
 * given volume, bucket, and directory structure. The tests include:
 *
 * 1. Test Root Level Search Restriction: Ensures searching at the root level returns a bad request.
 * 2. Test Volume Level Search Restriction: Ensures searching at the volume level returns a bad request.
 * 3. Test Bucket Level Search: Verifies search results within different types of buckets, both FSO and OBS.
 * 4. Test Directory Level Search: Validates searching inside specific directories.
 * 5. Test Key Level Search: Confirms search results for specific keys within buckets, both FSO and OBS.
 * 6. Test Key Level Search Under Directory: Verifies searching for keys within nested directories.
 * 7. Test Search Under Nested Directory: Checks search results within nested directories.
 * 8. Test Limit Search: Tests the limit functionality of the search API.
 * 9. Test Search Deleted Keys with Bad Request: Ensures bad requests with invalid params return correct responses.
 * 10. Test Last Key in Response: Confirms the presence of the last key in paginated responses.
 * 11. Test Search Deleted Keys with Pagination: Verifies paginated search results.
 * 12. Test Search in Empty Bucket: Checks the response for searching within an empty bucket.
 */
public class TestDeletedKeysSearchEndpoint extends AbstractReconSqlDBTest {

  @TempDir
  private Path temporaryFolder;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OMDBInsightSearchEndpoint deletedKeysSearchEndpoint;
  private OzoneConfiguration ozoneConfiguration;
  private static final String ROOT_PATH = "/";
  private OMMetadataManager omMetadataManager;

  @BeforeEach
  public void setUp() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setLong(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, 100);
    omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve("JunitOmDBDir")).toFile());
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(temporaryFolder.resolve("OmMetataDir")).toFile());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(OzoneStorageContainerManager.class,
                mock(OzoneStorageContainerManager.class))
            .addBinding(ReconNamespaceSummaryManager.class,
                mock(ReconNamespaceSummaryManager.class))
            .addBinding(OMDBInsightSearchEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .build();
    deletedKeysSearchEndpoint = reconTestInjector.getInstance(OMDBInsightSearchEndpoint.class);

    populateOMDB();
  }


  private static OMMetadataManager initializeNewOmMetadataManager(File omDbDir) throws IOException {
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS, omDbDir.getAbsolutePath());
    return new OmMetadataManagerImpl(omConfiguration, null);
  }

  @Test
  public void testRootLevelSearchRestriction() throws IOException {
    String rootPath = "/";
    Response response = deletedKeysSearchEndpoint.searchDeletedKeys(rootPath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    rootPath = "";
    response = deletedKeysSearchEndpoint.searchDeletedKeys(rootPath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testVolumeLevelSearchRestriction() throws IOException {
    String volumePath = "/vola";
    Response response = deletedKeysSearchEndpoint.searchDeletedKeys(volumePath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    volumePath = "/volb";
    response = deletedKeysSearchEndpoint.searchDeletedKeys(volumePath, 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testBucketLevelSearch() throws IOException {
    // Search inside FSO bucket
    Response response = deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb1", 20, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(7, result.getFsoKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb1", 2, "");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getFsoKeyInfoList().size());

    // Search inside OBS bucket
    response = deletedKeysSearchEndpoint.searchDeletedKeys("/volc/bucketc1", 20, "");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(9, result.getNonFSOKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys("/vola/nonexistentbucket", 20, "");
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testDirectoryLevelSearch() throws IOException {
    Response response = deletedKeysSearchEndpoint.searchDeletedKeys("/volc/bucketc1/dirc1", 20,
            "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(4, result.getNonFSOKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys("/volc/bucketc1/dirc2", 20,
            "");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getNonFSOKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys(
        "/volb/bucketb1/nonexistentdir", 20, "");
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testKeyLevelSearch() throws IOException {
    // FSO Bucket key-level search
    Response response =
        deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb1/fileb1", 10,
            "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb1/fileb2", 10,
            "");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());

    // Test with non-existent key
    response = deletedKeysSearchEndpoint.searchDeletedKeys(
        "/volb/bucketb1/nonexistentfile", 1, "");
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testKeyLevelSearchUnderDirectory() throws IOException {
    // FSO Bucket key-level search under directory
    Response response =
        deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb1/dir1/file1",
            10, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys(
        "/volb/bucketb1/dir1/nonexistentfile", 10, "");
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
        response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testSearchUnderNestedDirectory() throws IOException {
    // OBS Bucket nested directory search
    Response response =
        deletedKeysSearchEndpoint.searchDeletedKeys("/volc/bucketc1/dirc1", 20, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(4, result.getNonFSOKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys(
        "/volc/bucketc1/dirc1/dirc11", 20, "");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getNonFSOKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys(
        "/volc/bucketc1/dirc1/dirc11/dirc111", 20, "");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getNonFSOKeyInfoList().size());

    response = deletedKeysSearchEndpoint.searchDeletedKeys(
        "/volc/bucketc1/dirc1/dirc11/dirc111/nonexistentfile", 20, "");
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");

    response = deletedKeysSearchEndpoint.searchDeletedKeys(
        "/volc/bucketc1/dirc1/dirc11/nonexistentfile", 20, "");
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testLimitSearch() throws IOException {
    Response response = deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb1", 2, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getFsoKeyInfoList().size());
  }

  @Test
  public void testSearchDeletedKeysWithBadRequest() throws IOException {
    int negativeLimit = -1;
    Response response = deletedKeysSearchEndpoint.searchDeletedKeys("@323232", negativeLimit, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    response = deletedKeysSearchEndpoint.searchDeletedKeys("///", 20, "");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testLastKeyInResponse() throws IOException {
    Response response = deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb1", 20, "");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(7, result.getFsoKeyInfoList().size());
    assertEquals(result.getFsoKeyInfoList().get(6).getKey(), result.getLastKey(),
        "Expected last key to be 'fileb5'");
  }

  @Test
  public void testSearchDeletedKeysWithPagination() throws IOException {
    String startPrefix = "/volb/bucketb1";
    int limit = 2;
    String prevKey = "";

    Response response = deletedKeysSearchEndpoint.searchDeletedKeys(startPrefix, limit, prevKey);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getFsoKeyInfoList().size());

    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    response = deletedKeysSearchEndpoint.searchDeletedKeys(startPrefix, limit, prevKey);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getFsoKeyInfoList().size());

    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    response = deletedKeysSearchEndpoint.searchDeletedKeys(startPrefix, limit, prevKey);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getFsoKeyInfoList().size());
    assertEquals(result.getFsoKeyInfoList().get(1).getKey(),
        result.getLastKey(), "Expected last key to be empty");

    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    response = deletedKeysSearchEndpoint.searchDeletedKeys(startPrefix, limit, prevKey);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getFsoKeyInfoList().size());
    assertEquals(result.getFsoKeyInfoList().get(0).getKey(),
        result.getLastKey(), "Expected last key to be empty");
  }

  @Test
  public void testSearchInEmptyBucket() throws IOException {
    Response response =
        deletedKeysSearchEndpoint.searchDeletedKeys("/volb/bucketb2", 20, "");
    assertEquals(404, response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  private void populateOMDB() throws Exception {
    // Create FSO bucket
    createBucket("volb", "bucketb1", BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // Create OBS bucket
    createBucket("volc", "bucketc1", BucketLayout.OBJECT_STORE);

    createDeletedKey("fileb1", "bucketb1", "volb", 1000);
    createDeletedKey("fileb2", "bucketb1", "volb", 1000);
    createDeletedKey("fileb3", "bucketb1", "volb", 1000);
    createDeletedKey("fileb4", "bucketb1", "volb", 1000);
    createDeletedKey("fileb5", "bucketb1", "volb", 1000);

    createDeletedKey("dir1/file1", "bucketb1", "volb", 1000);
    createDeletedKey("dir1/file2", "bucketb1", "volb", 1000);

    createDeletedKey("dirc1/filec1", "bucketc1", "volc", 1000);
    createDeletedKey("dirc1/filec2", "bucketc1", "volc", 1000);
    createDeletedKey("dirc2/filec3", "bucketc1", "volc", 1000);
    createDeletedKey("dirc2/filec4", "bucketc1", "volc", 1000);
    createDeletedKey("dirc2/filec5", "bucketc1", "volc", 1000);
    createDeletedKey("dirc2/filgetec6", "bucketc1", "volc", 1000);
    createDeletedKey("dirc2/filec7", "bucketc1", "volc", 1000);

    // create nested directories and files in bucketc1
    createDeletedKey("dirc1/dirc11/filec11", "bucketc1", "volc", 1000);
    createDeletedKey("dirc1/dirc11/dirc111/filec111", "bucketc1", "volc", 1000);
  }

  private void createBucket(String volumeName, String bucketName, BucketLayout bucketLayout) throws Exception {
    String bucketKey = reconOMMetadataManager.getBucketKey(volumeName, bucketName);
    long bucketId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE; // Generate positive ID
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setObjectID(bucketId)
        .setBucketLayout(bucketLayout)
        .build();
    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo);
  }

  private void createDeletedKey(String keyName, String bucketName,
                                String volumeName, long dataSize) throws IOException {
    // Construct the deleted key path
    String deletedKey = "/" + volumeName + "/" + bucketName + "/" + keyName + "/" +
            UUID.randomUUID().getMostSignificantBits();

    // Create a list to hold OmKeyInfo objects
    List<OmKeyInfo> omKeyInfos = new ArrayList<>();

    // Build OmKeyInfo object
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .build();

    // Add the OmKeyInfo object to the list
    omKeyInfos.add(omKeyInfo);

    // Create a RepeatedOmKeyInfo object with the list of OmKeyInfo
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(omKeyInfos);

    // Write the deleted key information to the OM metadata manager
    writeDeletedKeysToOm(reconOMMetadataManager, deletedKey, repeatedOmKeyInfo);
  }

  /**
   * Writes deleted key information to the Ozone Manager metadata table.
   * @param omMetadataManager the Ozone Manager metadata manager
   * @param deletedKey the name of the deleted key
   * @param repeatedOmKeyInfo the RepeatedOmKeyInfo object containing key information
   * @throws IOException if there is an error accessing the metadata table
   */
  public static void writeDeletedKeysToOm(OMMetadataManager omMetadataManager,
                                          String deletedKey,
                                          RepeatedOmKeyInfo repeatedOmKeyInfo) throws IOException {
    // Put the deleted key information into the deleted table
    omMetadataManager.getDeletedTable().put(deletedKey, repeatedOmKeyInfo);
  }

}
