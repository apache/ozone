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
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.ws.rs.core.Response;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getBucketLayout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test for OMDBInsightSearchEndpoint.
 */
public class TestOMDBInsightSearchEndpoint extends AbstractReconSqlDBTest {
  @TempDir
  private Path temporaryFolder;
  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OMDBInsightSearchEndpoint omdbInsightSearchEndpoint;
  private Random random = new Random();
  private Set<Long> generatedIds = new HashSet<>();
  public TestOMDBInsightSearchEndpoint() {
    super();
  }

  private long generateUniqueRandomLong() {
    long newValue;
    do {
      newValue = random.nextLong();
    } while (generatedIds.contains(newValue));

    generatedIds.add(newValue);
    return newValue;
  }

  @BeforeEach
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve(
            "JunitOmMetadata")).toFile());
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(temporaryFolder.resolve(
            "JunitOmMetadataTest")).toFile());
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
    omdbInsightSearchEndpoint = reconTestInjector.getInstance(
        OMDBInsightSearchEndpoint.class);
  }

  @Test
  public void testSearchOpenKeys() throws Exception {
    // Create 3 keys in 'bucketOne' and 2 keys in 'bucketTwo'
    for (int i = 1; i <= 3; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketOne", "key_" + i, true);
      String keyPath = String.format("/sampleVol/%s/key_%d", "bucketOne", i);
      reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
          .put(keyPath, omKeyInfo);
    }

    for (int i = 1; i <= 2; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketTwo", "key_" + i, true);
      String keyPath = String.format("/sampleVol/%s/key_%d", "bucketTwo", i);
      reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
          .put(keyPath, omKeyInfo);
    }

    // Search for keys in 'bucketOne'
    String searchPrefixBucketOne = "/sampleVol/bucketOne/";
    Response responseBucketOne =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefixBucketOne, 10);

    // Assert that the search response for 'bucketOne' is OK and verify the results
    assertEquals(Response.Status.OK.getStatusCode(),
        responseBucketOne.getStatus());
    List<OmKeyInfo> searchResultsBucketOne =
        (List<OmKeyInfo>) responseBucketOne.getEntity();
    assertNotNull(searchResultsBucketOne);
    assertEquals(3, searchResultsBucketOne.size());

    searchResultsBucketOne.forEach(keyInfo -> {
      assertTrue(keyInfo.getKeyName().startsWith("key_"));
      assertTrue(keyInfo.getVolumeName().startsWith("sampleVol"));
      assertTrue(keyInfo.getBucketName().startsWith("bucketOne"));
    });

    // Search for keys in 'bucketTwo'
    String searchPrefixBucketTwo = "/sampleVol/bucketTwo/";
    Response responseBucketTwo =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefixBucketTwo, 10);

    // Assert that the search response for 'bucketTwo' is OK and verify the results
    assertEquals(Response.Status.OK.getStatusCode(),
        responseBucketTwo.getStatus());
    List<OmKeyInfo> searchResultsBucketTwo =
        (List<OmKeyInfo>) responseBucketTwo.getEntity();
    assertNotNull(searchResultsBucketTwo);
    assertEquals(2, searchResultsBucketTwo.size());

    searchResultsBucketTwo.forEach(keyInfo -> {
      assertTrue(keyInfo.getKeyName().startsWith("key_"));
      assertTrue(keyInfo.getVolumeName().startsWith("sampleVol"));
      assertTrue(keyInfo.getBucketName().startsWith("bucketTwo"));
    });
  }

  @Test
  public void testConvertToObjectPathForEmptyPrefix() throws Exception {
    String result = omdbInsightSearchEndpoint.convertToObjectPath("");
    assertEquals("", result, "Expected an empty string for empty input");
  }

  @Test
  public void testConvertToObjectPathForVolumeOnly() throws Exception {
    setupVolume("vol1", 100L);

    String result = omdbInsightSearchEndpoint.convertToObjectPath("/vol1");
    assertEquals("/100", result, "Incorrect conversion for volume only path");
  }

  @Test
  public void testConvertToObjectPathForVolumeAndBucket() throws Exception {
    setupVolume("vol1", 100L);
    setupBucket("vol1", "bucketOne", 200L, BucketLayout.LEGACY);

    String result =
        omdbInsightSearchEndpoint.convertToObjectPath("vol1/bucketOne");
    assertEquals("/100/200", result,
        "Incorrect conversion for volume and bucket path");
  }

  @Test
  public void testSearchOpenKeysWithDifferentBucketLayouts() throws Exception {
    // Setup for LEGACY bucket layout
    OmKeyInfo legacyKeyInfo =
        getOmKeyInfo("sampleVol", "legacyBucket", "legacy_key_1", true);
    String legacyKeyPath =
        String.format("/sampleVol/%s/%s", "legacyBucket", "legacy_key_1");
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.LEGACY)
        .put(legacyKeyPath, legacyKeyInfo);

    // Setup for FSO bucket layout
    OmKeyInfo fsoKeyInfo =
        getOmKeyInfo("sampleVol", "fsoBucket", "fso_key_1", true);
    setupBucket("sampleVol", "fsoBucket", 200L,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long fsoKeyObjectId = fsoKeyInfo.getObjectID();
    long bucketId = 200L;
    long volumeId = 0L;
    String fsoKeyPath = String.format("/%s/%s/%s", volumeId, bucketId,
        fsoKeyInfo.getObjectID());
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put(fsoKeyPath, fsoKeyInfo);

    // Search for keys under the LEGACY bucket
    String searchPrefixLegacy = "/sampleVol/legacyBucket/";
    Response responseLegacy =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefixLegacy, 10);

    // Verify response for LEGACY bucket layout
    assertEquals(Response.Status.OK.getStatusCode(),
        responseLegacy.getStatus());
    List<OmKeyInfo> searchResultsLegacy =
        (List<OmKeyInfo>) responseLegacy.getEntity();
    assertNotNull(searchResultsLegacy);
    assertEquals(1, searchResultsLegacy.size());
    assertTrue(
        searchResultsLegacy.get(0).getKeyName().startsWith("legacy_key_"));
    assertEquals("legacyBucket", searchResultsLegacy.get(0).getBucketName());

    // Search for keys under the FSO bucket
    String searchPrefixFso = "/sampleVol/fsoBucket/";
    Response responseFso =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefixFso, 10);

    // Verify response for FSO bucket layout
    assertEquals(Response.Status.OK.getStatusCode(), responseFso.getStatus());
    List<OmKeyInfo> searchResultsFso =
        (List<OmKeyInfo>) responseFso.getEntity();
    assertNotNull(searchResultsFso);
    assertEquals(1, searchResultsFso.size());
    assertTrue(searchResultsFso.get(0).getKeyName().startsWith("fso_key_"));
    assertEquals("fsoBucket", searchResultsFso.get(0).getBucketName());

    // Pass only the volume name and verify the response.
    String searchPrefixVolume = "/sampleVol/";
    Response responseVolume =
        omdbInsightSearchEndpoint.searchOpenKeys(searchPrefixVolume, 10);

    // Verify response for volume name
    assertEquals(Response.Status.OK.getStatusCode(),
        responseVolume.getStatus());
    List<OmKeyInfo> searchResultsVolume =
        (List<OmKeyInfo>) responseVolume.getEntity();
    assertNotNull(searchResultsVolume);
    assertEquals(2, searchResultsVolume.size());
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName, boolean isFile) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setFile(isFile)
        .setObjectID(generateUniqueRandomLong())
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(random.nextLong())
        .build();
  }

  public void setupVolume(String volumeName, long volumeId) throws Exception {
    Table<String, OmVolumeArgs> volumeTable =
        reconOMMetadataManager.getVolumeTable();

    OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("TestUser")
        .setOwnerName("TestUser")
        .setObjectID(volumeId)
        .build();
    // Insert the volume into the table
    volumeTable.put(reconOMMetadataManager.getVolumeKey(volumeName),
        omVolumeArgs);
  }

  private void setupBucket(String volumeName, String bucketName, long bucketId,
                           BucketLayout layout)
      throws Exception {
    Table<String, OmBucketInfo> bucketTable =
        reconOMMetadataManager.getBucketTable();

    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setObjectID(bucketId)
        .setBucketLayout(layout)
        .build();

    String bucketKey =
        reconOMMetadataManager.getBucketKey(volumeName, bucketName);
    bucketTable.put(bucketKey, omBucketInfo);
  }

}
