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
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
  private OMDBInsightEndpoint omdbInsightEndpoint;

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
    omdbInsightEndpoint = reconTestInjector.getInstance(OMDBInsightEndpoint.class);
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
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", rootPath);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testEmptySearchPrefix() throws IOException {
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(100, "", "");
    // In this case we get all the keys from the OMDB
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(16, result.getRepeatedOmKeyInfoList().size());

    // Set limit to 10 and pass empty search prefix
    response = omdbInsightEndpoint.getDeletedKeyInfo(10, "", "");
    // In this case we get all the keys from the OMDB
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(10, result.getRepeatedOmKeyInfoList().size());
  }

  @Test
  public void testVolumeLevelSearchRestriction() throws IOException {
    String volumePath = "/vola";
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", volumePath);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    volumePath = "/volb";
    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", volumePath);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testBucketLevelSearch() throws IOException {
    // Search inside FSO bucket
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volb/bucketb1");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(7, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(2, "", "/volb/bucketb1");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getRepeatedOmKeyInfoList().size());

    // Search inside OBS bucket
    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(9, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/vola/nonexistentbucket");
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testDirectoryLevelSearch() throws IOException {
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1/dirc1");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(4, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1/dirc2");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(5, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volb/bucketb1/nonexistentdir");
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testKeyLevelSearch() throws IOException {
    // FSO Bucket key-level search
    Response response =
        omdbInsightEndpoint.getDeletedKeyInfo(10, "", "/volb/bucketb1/fileb1");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result =
        (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getRepeatedOmKeyInfoList().size());

    response =
        omdbInsightEndpoint.getDeletedKeyInfo(10, "", "/volb/bucketb1/fileb2");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getRepeatedOmKeyInfoList().size());

    // Test with non-existent key
    response = omdbInsightEndpoint.getDeletedKeyInfo(1, "", "/volb/bucketb1/nonexistentfile");
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(),
        response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testKeyLevelSearchUnderDirectory() throws IOException {
    // FSO Bucket key-level search under directory
    Response response =
        omdbInsightEndpoint.getDeletedKeyInfo(10, "", "/volb/bucketb1/dir1/file1");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(10, "",
        "/volb/bucketb1/dir1/nonexistentfile");
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(),
        response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testSearchUnderNestedDirectory() throws IOException {
    // OBS Bucket nested directory search
    Response response =
        omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1/dirc1");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(4, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1/dirc1/dirc11");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1/dirc1/dirc11/dirc111");
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getRepeatedOmKeyInfoList().size());

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1/dirc1/dirc11/dirc111/nonexistentfile");
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volc/bucketc1/dirc1/dirc11/nonexistentfile");
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testLimitSearch() throws IOException {
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(2, "", "/volb/bucketb1");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getRepeatedOmKeyInfoList().size());
  }

  @Test
  public void testSearchDeletedKeysWithBadRequest() throws IOException {
    int negativeLimit = -1;
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(negativeLimit, "", "@323232");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");

    response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "///");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    entity = (String) response.getEntity();
    assertTrue(entity.contains("Invalid startPrefix: Path must be at the bucket level or deeper"),
        "Expected a message indicating the path must be at the bucket level or deeper");
  }

  @Test
  public void testLastKeyInResponse() throws IOException {
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volb/bucketb1");
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(7, result.getRepeatedOmKeyInfoList().size());

    // Compute the expected last key from the last entry in the result list
    String computedLastKey = "/" +
        result.getRepeatedOmKeyInfoList().get(6).getOmKeyInfoList().get(0).getVolumeName() + "/" +
        result.getRepeatedOmKeyInfoList().get(6).getOmKeyInfoList().get(0).getBucketName() + "/" +
        result.getRepeatedOmKeyInfoList().get(6).getOmKeyInfoList().get(0).getKeyName() + "/";

    // Check that the last key in the response starts with the expected value
    assertTrue(result.getLastKey().startsWith(computedLastKey));
  }

  @Test
  public void testSearchDeletedKeysWithPagination() throws IOException {
    String startPrefix = "/volb/bucketb1";
    int limit = 2;
    String prevKey = "";

    Response response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getRepeatedOmKeyInfoList().size());

    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getRepeatedOmKeyInfoList().size());

    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(2, result.getRepeatedOmKeyInfoList().size());

    prevKey = result.getLastKey();
    assertNotNull(prevKey, "Last key should not be null");

    response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    result = (KeyInsightInfoResponse) response.getEntity();
    assertEquals(1, result.getRepeatedOmKeyInfoList().size());
    // Compute the expected last key from the last entry in the result list
    String computedLastKey = "/" +
        result.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0)
            .getVolumeName() + "/" +
        result.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0)
            .getBucketName() + "/" +
        result.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0)
            .getKeyName() + "/";

    // Check that the last key in the response starts with the expected value
    assertTrue(result.getLastKey().startsWith(computedLastKey));
  }

  @Test
  public void testSearchInEmptyBucket() throws IOException {
    Response response = omdbInsightEndpoint.getDeletedKeyInfo(20, "", "/volb/bucketb2");
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    String entity = (String) response.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testPrevKeyProvidedStartPrefixEmpty() throws IOException {
    // Case 1: prevKey provided, startPrefix empty
    // Seek to the prevKey, skip the first matching record, then return remaining records until limit is reached.
    String prevKey = "/volb/bucketb1/fileb3"; // This key exists, will skip it
    int limit = 3;
    String startPrefix = ""; // Empty startPrefix

    Response response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();

    // Assert that we get the next 3 records after skipping the prevKey
    assertEquals(3, result.getRepeatedOmKeyInfoList().size());
    assertEquals("fileb4", result.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0).getKeyName());
  }

  @Test
  public void testPrevKeyEmptyStartPrefixEmpty() throws IOException {
    // Case 2: prevKey empty, startPrefix empty
    // No need to seek, start from the first record and return records until limit is reached.
    String prevKey = ""; // Empty prevKey
    int limit = 100;
    String startPrefix = ""; // Empty startPrefix

    Response response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();

    // Assert that we get all the 16 records currently in the deleted keys table
    assertEquals(16, result.getRepeatedOmKeyInfoList().size());
  }

  @Test
  public void testPrevKeyEmptyStartPrefixProvided() throws IOException {
    // Case 3: prevKey empty, startPrefix provided
    // Seek to the startPrefix and return matching records until limit is reached.
    String prevKey = ""; // Empty prevKey
    int limit = 2;
    String startPrefix = "/volb/bucketb1/fileb"; // Seek to startPrefix and match files

    Response response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();

    // Assert that we get the first 2 records that match startPrefix
    assertEquals(2, result.getRepeatedOmKeyInfoList().size());
    assertEquals("fileb1", result.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0).getKeyName());
  }

  @Test
  public void testPrevKeyProvidedStartPrefixProvided() throws IOException {
    // Case 4: prevKey provided, startPrefix provided
    // Seek to the prevKey, skip it, and return remaining records matching startPrefix until limit is reached.
    String prevKey = "/volb/bucketb1/fileb2"; // This key exists, will skip it
    int limit = 3;
    String startPrefix = "/volb/bucketb1"; // Matching prefix

    Response response = omdbInsightEndpoint.getDeletedKeyInfo(limit, prevKey, startPrefix);
    assertEquals(200, response.getStatus());
    KeyInsightInfoResponse result = (KeyInsightInfoResponse) response.getEntity();

    // Assert that we get the next 2 records that match startPrefix after skipping prevKey having fileb2
    assertEquals(3, result.getRepeatedOmKeyInfoList().size());
    assertEquals("fileb3", result.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0).getKeyName());
  }

  /**
   * Populates the OMDB with a set of deleted keys for testing purposes.
   * This diagram is for reference:
   * * root
   *   ├── volb (Total Size: 7000KB)
   *   │   ├── bucketb1 (Total Size: 7000KB)
   *   │   │   ├── fileb1 (Size: 1000KB)
   *   │   │   ├── fileb2 (Size: 1000KB)
   *   │   │   ├── fileb3 (Size: 1000KB)
   *   │   │   ├── fileb4 (Size: 1000KB)
   *   │   │   ├── fileb5 (Size: 1000KB)
   *   │   │   ├── dir1 (Total Size: 2000KB)
   *   │   │   │   ├── file1 (Size: 1000KB)
   *   │   │   │   └── file2 (Size: 1000KB)
   *   ├── volc (Total Size: 9000KB)
   *   │   ├── bucketc1 (Total Size: 9000KB)
   *   │   │   ├── dirc1 (Total Size: 4000KB)
   *   │   │   │   ├── filec1 (Size: 1000KB)
   *   │   │   │   ├── filec2 (Size: 1000KB)
   *   │   │   │   ├── dirc11 (Total Size: 2000KB)
   *   │   │   │       ├── filec11 (Size: 1000KB)
   *   │   │   │       └── dirc111 (Total Size: 1000KB)
   *   │   │   │           └── filec111 (Size: 1000KB)
   *   │   │   ├── dirc2 (Total Size: 5000KB)
   *   │   │   │   ├── filec3 (Size: 1000KB)
   *   │   │   │   ├── filec4 (Size: 1000KB)
   *   │   │   │   ├── filec5 (Size: 1000KB)
   *   │   │   │   ├── filec6 (Size: 1000KB)
   *   │   │   │   └── filec7 (Size: 1000KB)
   *
   * @throws Exception if an error occurs while creating deleted keys.
   */
  private void populateOMDB() throws Exception {

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
        .setObjectID(UUID.randomUUID().getMostSignificantBits())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .build();

    // Add the OmKeyInfo object to the list
    omKeyInfos.add(omKeyInfo);

    // Create a RepeatedOmKeyInfo object with the list of OmKeyInfo
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(omKeyInfos, 1);

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
