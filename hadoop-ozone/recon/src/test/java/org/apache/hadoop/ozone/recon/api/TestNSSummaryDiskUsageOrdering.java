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
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProviderWithFSO;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
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
 * Test NSSummary Disk Usage subpath ordering.
 */
public class TestNSSummaryDiskUsageOrdering {

  @TempDir
  private Path temporaryFolder;

  private ReconOMMetadataManager reconOMMetadataManager;
  private NSSummaryEndpoint nsSummaryEndpoint;
  private static final String ROOT_PATH = "/";
  private static final String TEST_USER = "TestUser";

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setLong(OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD,
        100);
    OMMetadataManager omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve("JunitOmDBDir"))
            .toFile());
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        getMockOzoneManagerServiceProviderWithFSO();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(temporaryFolder.resolve("OmMetataDir")).toFile());

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
    ReconNamespaceSummaryManager reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    nsSummaryEndpoint = reconTestInjector.getInstance(NSSummaryEndpoint.class);

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
  public void testDiskUsageOrderingForRoot() throws Exception {
    // root level DU
    // Verify the ordering of subpaths under the root
    verifyOrdering(ROOT_PATH);
  }

  @Test
  public void testDiskUsageOrderingForVolume() throws Exception {
    // volume level DU
    // Verify the ordering of subpaths under the volume
    verifyOrdering("/volA");
    verifyOrdering("/volB");
  }

  @Test
  public void testDiskUsageOrderingForBucket() throws Exception {
    // bucket level DU
    // Verify the ordering of subpaths under the bucket
    verifyOrdering("/volA/bucketA1");
    verifyOrdering("/volA/bucketA2");
    verifyOrdering("/volA/bucketA3");
    verifyOrdering("/volB/bucketB1");
  }

  private void verifyOrdering(String path)
      throws IOException {
    Response response =
        nsSummaryEndpoint.getDiskUsage(path, true, false, true);
    DUResponse duRes = (DUResponse) response.getEntity();
    List<DUResponse.DiskUsage> duData = duRes.getDuData();
    List<DUResponse.DiskUsage> sortedDuData = new ArrayList<>(duData);
    // Sort the DU data by size in descending order to compare with the original.
    sortedDuData.sort(
        Comparator.comparingLong(DUResponse.DiskUsage::getSize).reversed());

    for (int i = 0; i < duData.size(); i++) {
      assertEquals(sortedDuData.get(i).getSubpath(),
          duData.get(i).getSubpath(),
          "DU-Sub-Path under " + path +
              " should be sorted by descending order of size");
    }
  }

  /**
   * Tests the NSSummaryEndpoint for a given volume, bucket, and directory structure.
   * The test setup mimics the following filesystem structure with specified sizes:
   *
   * root
   * ├── volA
   * │   ├── bucketA1
   * │   │   ├── fileA1 (Size: 600KB)
   * │   │   ├── fileA2 (Size: 80KB)
   * │   │   ├── dirA1 (Total Size: 1500KB)
   * │   │   ├── dirA2 (Total Size: 1700KB)
   * │   │   └── dirA3 (Total Size: 1300KB)
   * │   ├── bucketA2
   * │   │   ├── fileA3 (Size: 200KB)
   * │   │   ├── fileA4 (Size: 4000KB)
   * │   │   ├── dirA4 (Total Size: 1100KB)
   * │   │   ├── dirA5 (Total Size: 1900KB)
   * │   │   └── dirA6 (Total Size: 210KB)
   * │   └── bucketA3
   * │       ├── fileA5 (Size: 5000KB)
   * │       ├── fileA6 (Size: 700KB)
   * │       ├── dirA7 (Total Size: 1200KB)
   * │       ├── dirA8 (Total Size: 1600KB)
   * │       └── dirA9 (Total Size: 180KB)
   * └── volB
   *     └── bucketB1
   *         ├── fileB1 (Size: 300KB)
   *         ├── fileB2 (Size: 500KB)
   *         ├── dirB1 (Total Size: 14000KB)
   *         ├── dirB2 (Total Size: 1800KB)
   *         └── dirB3 (Total Size: 2200KB)
   *
   * @throws Exception
   */
  private void populateOMDB() throws Exception {
    // Create Volumes
    long volAObjectId = createVolume("volA");
    long volBObjectId = createVolume("volB");

    // Create Buckets in volA
    long bucketA1ObjectId =
        createBucket("volA", "bucketA1", 600 + 80 + 1500 + 1700 + 1300);
    long bucketA2ObjectId =
        createBucket("volA", "bucketA2", 200 + 4000 + 1100 + 1900 + 210);
    long bucketA3ObjectId =
        createBucket("volA", "bucketA3", 5000 + 700 + 1200 + 1600 + 180);

    // Create Bucket in volB
    long bucketB1ObjectId =
        createBucket("volB", "bucketB1", 300 + 500 + 14000 + 1800 + 2200);

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
    createFile("fileA1", "bucketA1", "volA", "fileA1", bucketA1ObjectId,
        bucketA1ObjectId, volAObjectId, 600 * 1024);
    createFile("fileA2", "bucketA1", "volA", "fileA2", bucketA1ObjectId,
        bucketA1ObjectId, volAObjectId, 80 * 1024);

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
    createFile("fileA3", "bucketA2", "volA", "fileA3", bucketA2ObjectId,
        bucketA2ObjectId, volAObjectId, 200 * 1024);
    createFile("fileA4", "bucketA2", "volA", "fileA4", bucketA2ObjectId,
        bucketA2ObjectId, volAObjectId, 4000 * 1024);

    // Create Directories and Files under bucketA3
    long dirA7ObjectId =
        createDirectory(bucketA3ObjectId, bucketA3ObjectId, volAObjectId,
            "dirA7");
    long dirA8ObjectId =
        createDirectory(bucketA3ObjectId, bucketA3ObjectId, volAObjectId,
            "dirA8");
    long dirA9ObjectId =
        createDirectory(bucketA3ObjectId, bucketA3ObjectId, volAObjectId,
            "dirA9");

    // Files directly under bucketA3
    createFile("fileA5", "bucketA3", "volA", "fileA5", bucketA3ObjectId,
        bucketA3ObjectId, volAObjectId, 5000 * 1024);
    createFile("fileA6", "bucketA3", "volA", "fileA6", bucketA3ObjectId,
        bucketA3ObjectId, volAObjectId, 700 * 1024);

    // Create Directories and Files under bucketB1
    long dirB1ObjectId =
        createDirectory(bucketB1ObjectId, bucketB1ObjectId, volBObjectId,
            "dirB1");
    long dirB2ObjectId =
        createDirectory(bucketB1ObjectId, bucketB1ObjectId, volBObjectId,
            "dirB2");
    long dirB3ObjectId =
        createDirectory(bucketB1ObjectId, bucketB1ObjectId, volBObjectId,
            "dirB3");

    // Files directly under bucketB1
    createFile("fileB1", "bucketB1", "volB", "fileB1", bucketB1ObjectId,
        bucketB1ObjectId, volBObjectId, 300 * 1024);
    createFile("fileB2", "bucketB1", "volB", "fileB2", bucketB1ObjectId,
        bucketB1ObjectId, volBObjectId, 500 * 1024);

    // Create Inner files under directories
    createFile("dirA1/innerFile", "bucketA1", "volA", "innerFile",
        dirA1ObjectId, bucketA1ObjectId, volAObjectId, 1500 * 1024);
    createFile("dirA2/innerFile", "bucketA1", "volA", "innerFile",
        dirA2ObjectId, bucketA1ObjectId, volAObjectId, 1700 * 1024);
    createFile("dirA3/innerFile", "bucketA1", "volA", "innerFile",
        dirA3ObjectId, bucketA1ObjectId, volAObjectId, 1300 * 1024);
    createFile("dirA4/innerFile", "bucketA2", "volA", "innerFile",
        dirA4ObjectId, bucketA2ObjectId, volAObjectId, 1100 * 1024);
    createFile("dirA5/innerFile", "bucketA2", "volA", "innerFile",
        dirA5ObjectId, bucketA2ObjectId, volAObjectId, 1900 * 1024);
    createFile("dirA6/innerFile", "bucketA2", "volA", "innerFile",
        dirA6ObjectId, bucketA2ObjectId, volAObjectId, 210 * 1024);
    createFile("dirA7/innerFile", "bucketA3", "volA", "innerFile",
        dirA7ObjectId, bucketA3ObjectId, volAObjectId, 1200 * 1024);
    createFile("dirA8/innerFile", "bucketA3", "volA", "innerFile",
        dirA8ObjectId, bucketA3ObjectId, volAObjectId, 1600 * 1024);
    createFile("dirA9/innerFile", "bucketA3", "volA", "innerFile",
        dirA9ObjectId, bucketA3ObjectId, volAObjectId, 180 * 1024);
    createFile("dirB1/innerFile", "bucketB1", "volB", "innerFile",
        dirB1ObjectId, bucketB1ObjectId, volBObjectId, 14000 * 1024);
    createFile("dirB2/innerFile", "bucketB1", "volB", "innerFile",
        dirB2ObjectId, bucketB1ObjectId, volBObjectId, 1800 * 1024);
    createFile("dirB3/innerFile", "bucketB1", "volB", "innerFile",
        dirB3ObjectId, bucketB1ObjectId, volBObjectId, 2200 * 1024);
  }

  /**
   * Create a volume and add it to the Volume Table.
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
   * @return bucket Object ID
   * @throws IOException
   */
  private long createBucket(String volumeName, String bucketName, long dataSize)
      throws Exception {
    String bucketKey =
        reconOMMetadataManager.getBucketKey(volumeName, bucketName);
    long bucketId = UUID.randomUUID().getMostSignificantBits() &
        Long.MAX_VALUE; // Generate positive ID
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setObjectID(bucketId)
        .setBucketLayout(getBucketLayout())
        .setUsedBytes(dataSize)
        .build();

    reconOMMetadataManager.getBucketTable().put(bucketKey, bucketInfo);
    return bucketId;
  }

  /**
   * Create a directory and add it to the Directory Table.
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
   * Create a file and add it to the File Table.
   * @return file Object ID
   * @throws IOException
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private long createFile(String key,
                          String bucket,
                          String volume,
                          String fileName,
                          long parentObjectId,
                          long bucketObjectId,
                          long volumeObjectId,
                          long dataSize) throws IOException {
    long objectId = UUID.randomUUID().getMostSignificantBits() &
        Long.MAX_VALUE; // Ensure positive ID
    writeKeyToOm(reconOMMetadataManager, key, bucket, volume, fileName,
        objectId,
        parentObjectId, bucketObjectId, volumeObjectId, dataSize,
        getBucketLayout());
    return objectId;
  }

  private static ReconStorageContainerManagerFacade getMockReconSCM()
      throws ContainerNotFoundException {
    ReconStorageContainerManagerFacade reconSCM =
        mock(ReconStorageContainerManagerFacade.class);
    ContainerManager containerManager = mock(ContainerManager.class);

    when(reconSCM.getContainerManager()).thenReturn(containerManager);
    ReconNodeManager mockReconNodeManager = mock(ReconNodeManager.class);
    when(reconSCM.getScmNodeManager()).thenReturn(mockReconNodeManager);
    return reconSCM;
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
