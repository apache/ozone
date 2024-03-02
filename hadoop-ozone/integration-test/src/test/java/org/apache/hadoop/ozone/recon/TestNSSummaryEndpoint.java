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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Comparator;
import java.util.function.BiConsumer;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

/**
 * Integration test for verifying the correctness of NSSummaryEndpoint.
 */
public class TestNSSummaryEndpoint {

  private static boolean omRatisEnabled = true;

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static String volumeName;
  private static String bucketName;
  private static OzoneClient client;

  private static final String VOLUME_A = "vola";
  private static final String VOLUME_B = "volb";
  private static final String BUCKET_A1 = "bucketa1";
  private static final String BUCKET_A2 = "bucketa2";
  private static final String BUCKET_A3 = "bucketa3";
  private static final String BUCKET_B1 = "bucketb1";

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .includeRecon(true)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client,
        getFSOBucketLayout());
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
  }

  @AfterAll
  public static void teardown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @AfterEach
  public void cleanup() {
    assertDoesNotThrow(() -> {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    });
  }

  /**
   * Tests the NSSummaryEndpoint for a given volume, bucket, and directory structure.
   * The test setup mimics the following filesystem structure with specified sizes:
   *
   * root
   * ├── volA
   * │   ├── bucketA1
   * │   │   ├── fileA1 (Size: 600KB)
   * │   │   ├── fileA2 (Size: 800KB)
   * │   │   ├── dirA1 (Total Size: 1500KB)
   * │   │   ├── dirA2 (Total Size: 1700KB)
   * │   │   └── dirA3 (Total Size: 1300KB)
   * │   ├── bucketA2
   * │   │   ├── fileA3 (Size: 200KB)
   * │   │   ├── fileA4 (Size: 400KB)
   * │   │   ├── dirA4 (Total Size: 1100KB)
   * │   │   ├── dirA5 (Total Size: 1900KB)
   * │   │   └── dirA6 (Total Size: 2100KB)
   * │   └── bucketA3
   * │       ├── fileA5 (Size: 500KB)
   * │       ├── fileA6 (Size: 700KB)
   * │       ├── dirA7 (Total Size: 1200KB)
   * │       ├── dirA8 (Total Size: 1600KB)
   * │       └── dirA9 (Total Size: 1800KB)
   * └── volB
   *     └── bucketB1
   *         ├── fileB1 (Size: 300KB)
   *         ├── fileB2 (Size: 500KB)
   *         ├── dirB1 (Total Size: 1400KB)
   *         ├── dirB2 (Total Size: 1800KB)
   *         └── dirB3 (Total Size: 2200KB)
   *
   * @throws Exception
   */
  @Test
  public void testDiskUsageOrderingBySubpathSize() throws Exception {
    // Setup test data and sync data from OM to Recon
    setupTestData();
    syncDataFromOM();

    OzoneStorageContainerManager reconSCM =
        cluster.getReconServer().getReconStorageContainerManager();
    ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager =
        (ReconNamespaceSummaryManagerImpl) cluster.getReconServer()
            .getReconNamespaceSummaryManager();
    ReconOMMetadataManager reconOmMetadataManagerInstance =
        (ReconOMMetadataManager) cluster.getReconServer()
            .getOzoneManagerServiceProvider().getOMMetadataManagerInstance();

    NSSummaryEndpoint nsSummaryEndpoint =
        new NSSummaryEndpoint(reconNamespaceSummaryManager,
            reconOmMetadataManagerInstance, reconSCM);

    // Verify the ordering of subpaths under the root
    verifyOrdering(nsSummaryEndpoint, "/");

    // Verify the ordering of subpaths under each volume
    verifyOrdering(nsSummaryEndpoint, VOLUME_A);
    verifyOrdering(nsSummaryEndpoint, VOLUME_B);

    // Verify the ordering of subpaths under each bucket
    verifyOrdering(nsSummaryEndpoint, VOLUME_A + "/" + BUCKET_A1);
    verifyOrdering(nsSummaryEndpoint, VOLUME_A + "/" + BUCKET_A2);
    verifyOrdering(nsSummaryEndpoint, VOLUME_A + "/" + BUCKET_A3);
    verifyOrdering(nsSummaryEndpoint, VOLUME_B + "/" + BUCKET_B1);
  }

  private void verifyOrdering(NSSummaryEndpoint nsSummaryEndpoint, String path)
      throws IOException {
    Response response =
        nsSummaryEndpoint.getDiskUsage(path, true, false);
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

  public void setupTestData() throws IOException {
    // Helper method to write data to a file
    BiConsumer<String, Long> writeFile = (filePath, size) -> {
      try (FSDataOutputStream outputStream = fs.create(new Path(filePath))) {
        byte[] data = new byte[size.intValue()];
        new Random().nextBytes(data); // Fill with random data
        outputStream.write(data);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    // Create volumes and buckets.
    client.getObjectStore().createVolume(VOLUME_A);
    OzoneVolume volumeA = client.getObjectStore().getVolume(VOLUME_A);
    volumeA.createBucket(BUCKET_A1);
    volumeA.createBucket(BUCKET_A2);
    volumeA.createBucket(BUCKET_A3);

    client.getObjectStore().createVolume(VOLUME_B);
    OzoneVolume volumeB = client.getObjectStore().getVolume(VOLUME_B);
    volumeB.createBucket(BUCKET_B1);

    // Define the structure and size in KB for the filesystem
    Map<String, Integer> fileSystemStructure = new LinkedHashMap<>();
    fileSystemStructure.put("vola/bucketa1/fileA1", 600);
    fileSystemStructure.put("vola/bucketa1/fileA2", 800);
    fileSystemStructure.put("vola/bucketa1/dirA1", 1500);
    fileSystemStructure.put("vola/bucketa1/dirA2", 1700);
    fileSystemStructure.put("vola/bucketa1/dirA3", 1300);
    fileSystemStructure.put("vola/bucketa2/fileA3", 200);
    fileSystemStructure.put("vola/bucketa2/fileA4", 400);
    fileSystemStructure.put("vola/bucketa2/dirA4", 1100);
    fileSystemStructure.put("vola/bucketa2/dirA5", 1900);
    fileSystemStructure.put("vola/bucketa2/dirA6", 2100);
    fileSystemStructure.put("vola/bucketa3/fileA5", 500);
    fileSystemStructure.put("vola/bucketa3/fileA6", 700);
    fileSystemStructure.put("vola/bucketa3/dirA7", 1200);
    fileSystemStructure.put("vola/bucketa3/dirA8", 1600);
    fileSystemStructure.put("vola/bucketa3/dirA9", 1800);
    fileSystemStructure.put("volb/bucketb1/fileB1", 300);
    fileSystemStructure.put("volb/bucketb1/fileB2", 500);
    fileSystemStructure.put("volb/bucketb1/dirB1", 1400);
    fileSystemStructure.put("volb/bucketb1/dirB2", 1800);
    fileSystemStructure.put("volb/bucketb1/dirB3", 2200);

    // Create files and directories
    for (Map.Entry<String, Integer> entry : fileSystemStructure.entrySet()) {
      String[] pathParts = entry.getKey().split("/");
      String itemName = pathParts[2];
      int sizeInKB = entry.getValue();
      // Calculate the size in bytes
      long sizeInBytes = sizeInKB * 1024L;

      if (itemName.startsWith("file")) {
        // Create a file with the specified size
        String filePath = "/" + volumeName + "/" + bucketName + "/" + itemName;
        writeFile.accept(filePath, sizeInBytes);
      } else {
        // Create a directory
        String dirPath = "/" + volumeName + "/" + bucketName + "/" + itemName;
        fs.mkdirs(new Path(dirPath));

        // Create a file inside the directory to achieve the total specified size
        String innerFilePath = dirPath + "/innerFile";
        writeFile.accept(innerFilePath, sizeInBytes);
      }
    }
  }

  private void syncDataFromOM() {
    // Sync data from Ozone Manager to Recon.
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
  }

  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

}

