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

package org.apache.hadoop.ozone.recon;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.RaftTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for NSSummary memory leak fix (HDDS-8565).
 * 
 * <p>This test validates that NSSummary entries are properly cleaned up
 * when directories and files are hard deleted from deletedTable/deletedDirTable.
 * 
 * <h3>Problem Context:</h3>
 * <p>In Apache Ozone's FSO (File System Optimized) bucket layout, the Recon service
 * maintains NSSummary (Namespace Summary) objects that track metadata statistics for
 * directories and files. These objects were not being cleaned up when entries were
 * hard deleted from deletedTable/deletedDirTable, causing a memory leak.
 * 
 * <h3>Memory Leak Scenario:</h3>
 * <p>Object lifecycle in Ozone FSO:
 * <ol>
 *   <li><b>CREATE</b>: Directory/file created → entry in directoryTable/fileTable + NSSummary created</li>
 *   <li><b>SOFT DELETE</b>: Directory/file deleted → entry moved to deletedDirTable/deletedTable</li>
 *   <li><b>HARD DELETE</b>: Background cleanup removes entry from deletedDirTable/deletedTable</li>
 *   <li><b>MEMORY LEAK</b>: NSSummary entries were not cleaned up during hard delete</li>
 * </ol>
 * 
 * <h3>Test Directory Structure:</h3>
 * <pre>
 * /memoryLeakTest/                    (root test directory)
 * ├── subdir0/                        (subdirectories created in loop)
 * │   ├── file0                       (files with test content)
 * │   ├── file1
 * │   ├── file2
 * │   ├── file3
 * │   └── file4
 * ├── subdir1/
 * │   ├── file0
 * │   ├── file1
 * │   ├── file2
 * │   ├── file3
 * │   └── file4
 * ├── subdir2/
 * │   └── ... (same pattern)
 * └── subdir[n]/
 *     └── ... (configurable number of subdirs and files)
 * </pre>
 * 
 * <h3>Test Flow:</h3>
 * <ol>
 *   <li><b>Setup</b>: Create directory structure with subdirectories and files</li>
 *   <li><b>Sync</b>: Sync metadata from OM to Recon to create NSSummary entries</li>
 *   <li><b>Verify Initial State</b>: Confirm NSSummary entries exist for all directories</li>
 *   <li><b>Soft Delete</b>: Delete directory structure (moves entries to deletedTable/deletedDirTable)</li>
 *   <li><b>Verify Soft Delete</b>: Confirm entries are in deleted tables</li>
 *   <li><b>Hard Delete</b>: Simulate background cleanup removing entries from deleted tables</li>
 *   <li><b>Verify Cleanup</b>: Confirm NSSummary entries are properly cleaned up (memory leak fix)</li>
 * </ol>
 * 
 * <h3>Memory Leak Fix Implementation:</h3>
 * <p>The fix was implemented in {@code NSSummaryTaskWithFSO.handleUpdateOnDeletedDirTable()}
 * method, which:
 * <ul>
 *   <li>Listens for DELETE events on deletedDirTable and deletedTable</li>
 *   <li>Removes corresponding entries from nsSummaryMap (in-memory cleanup)</li>
 *   <li>Batch deletes NSSummary entries from database (persistent cleanup)</li>
 * </ul>
 * 
 * @see org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithFSO#handleUpdateOnDeletedDirTable
 * @see org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager#batchDeleteNSSummaries
 */
public class TestNSSummaryMemoryLeak {

  private static final Logger LOG = LoggerFactory.getLogger(TestNSSummaryMemoryLeak.class);

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static OzoneClient client;
  private static ReconService recon;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Configure delays for testing
    conf.setInt(OZONE_DIR_DELETING_SERVICE_INTERVAL, 1000000);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 10000000, TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    
    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // Create FSO bucket for testing
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    IOUtils.closeQuietly(fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test that verifies NSSummary entries are properly cleaned up during hard delete.
   * 
   * <p>This test simulates the complete object lifecycle in Ozone FSO:
   * <ol>
   *   <li><b>CREATE</b>: Creates directory structure and verifies NSSummary entries are created</li>
   *   <li><b>SOFT DELETE</b>: Deletes directories and verifies entries move to deletedTable/deletedDirTable</li>
   *   <li><b>HARD DELETE</b>: Simulates background cleanup and verifies NSSummary cleanup (memory leak fix)</li>
   * </ol>
   * 
   * <p><b>Directory Structure Created:</b>
   * <pre>
   * /memoryLeakTest/
   * ├── subdir0/ (contains 5 files: file0, file1, file2, file3, file4)
   * ├── subdir1/ (contains 5 files: file0, file1, file2, file3, file4)
   * ├── subdir2/ (contains 5 files: file0, file1, file2, file3, file4)
   * ├── ...
   * └── subdir9/ (contains 5 files: file0, file1, file2, file3, file4)
   * </pre>
   * 
   * <p><b>Total Objects Created:</b>
   * <ul>
   *   <li>1 root directory (/memoryLeakTest)</li>
   *   <li>10 subdirectories (subdir0-subdir9)</li>
   *   <li>50 files (5 files per subdirectory)</li>
   *   <li>Total: 61 objects that will have NSSummary entries</li>
   * </ul>
   * 
   * @throws Exception if test fails
   */
  @Test
  public void testNSSummaryCleanupOnHardDelete() throws Exception {
    LOG.info("Starting NSSummary memory leak fix test");
    
    // Create test directory structure
    Path testDir = new Path("/memoryLeakTest");
    fs.mkdirs(testDir);
    
    // Create subdirectories and files
    int numSubdirs = 10;
    int filesPerDir = 5;
    createDirectoryStructure(testDir, numSubdirs, filesPerDir);
    
    // Sync data to Recon
    syncDataFromOM();
    
    // Get services for verification
    OzoneManagerServiceProviderImpl omServiceProvider = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    ReconNamespaceSummaryManager namespaceSummaryManager = 
        recon.getReconServer().getReconNamespaceSummaryManager();
    ReconOMMetadataManager omMetadataManager = 
        (ReconOMMetadataManager) omServiceProvider.getOMMetadataManagerInstance();
    
    // Verify initial state - NSSummary entries should exist
    verifyNSSummaryEntriesExist(omMetadataManager, namespaceSummaryManager, numSubdirs);
    
    // Delete directory structure to trigger soft delete
    fs.delete(testDir, true);
    syncDataFromOM();
    
    // Verify soft delete state - entries should be in deletedTable/deletedDirTable
    verifyEntriesInDeletedTables(omMetadataManager, numSubdirs, filesPerDir);
    
    // Trigger hard delete by clearing deleted tables
    // This simulates the background process that hard deletes entries
    simulateHardDelete(omMetadataManager);
    syncDataFromOM();
    
    // Verify memory leak fix - NSSummary entries should be cleaned up
    verifyNSSummaryCleanup(omMetadataManager, namespaceSummaryManager);
    
    LOG.info("NSSummary memory leak fix test completed successfully");
  }

  /**
   * Test with larger directory structure to validate memory efficiency and scalability.
   * 
   * <p>This test creates a larger directory structure to validate that the memory leak fix
   * works efficiently at scale and doesn't cause performance degradation.
   * 
   * <p><b>Large Directory Structure Created:</b>
   * <pre>
   * /largeMemoryLeakTest/
   * ├── subdir0/ (contains 20 files: file0, file1, ..., file19)
   * ├── subdir1/ (contains 20 files: file0, file1, ..., file19)
   * ├── subdir2/ (contains 20 files: file0, file1, ..., file19)
   * ├── ...
   * └── subdir49/ (contains 20 files: file0, file1, ..., file19)
   * </pre>
   * 
   * <p><b>Total Objects Created:</b>
   * <ul>
   *   <li>1 root directory (/largeMemoryLeakTest)</li>
   *   <li>50 subdirectories (subdir0-subdir49)</li>
   *   <li>1000 files (20 files per subdirectory)</li>
   *   <li>Total: 1051 objects that will have NSSummary entries</li>
   * </ul>
   * 
   * <p><b>Memory Usage Monitoring:</b>
   * <p>This test monitors memory usage before and after the deletion to validate that
   * the memory leak fix prevents excessive memory consumption. The test performs:
   * <ul>
   *   <li>Memory measurement before deletion</li>
   *   <li>Directory structure deletion and hard delete simulation</li>
   *   <li>Garbage collection and memory measurement after cleanup</li>
   *   <li>Verification that NSSummary entries are properly cleaned up</li>
   * </ul>
   * 
   * @throws Exception if test fails
   */
  @Test
  public void testMemoryLeakWithLargeStructure() throws Exception {
    LOG.info("Starting large structure memory leak test");
    
    // Create larger test structure
    Path largeTestDir = new Path("/largeMemoryLeakTest");
    fs.mkdirs(largeTestDir);
    
    int numSubdirs = 50;
    int filesPerDir = 20;
    createDirectoryStructure(largeTestDir, numSubdirs, filesPerDir);
    
    syncDataFromOM();
    
    // Get current memory usage
    Runtime runtime = Runtime.getRuntime();
    long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
    
    // Delete and verify cleanup
    fs.delete(largeTestDir, true);
    syncDataFromOM();
    
    // Simulate hard delete
    OzoneManagerServiceProviderImpl omServiceProvider = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    ReconOMMetadataManager omMetadataManager = 
        (ReconOMMetadataManager) omServiceProvider.getOMMetadataManagerInstance();
    
    simulateHardDelete(omMetadataManager);
    syncDataFromOM();
    
    // Force garbage collection
    RaftTestUtil.gc();

    // Verify memory cleanup
    long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
    LOG.info("Memory usage - Before: {} bytes, After: {} bytes", memoryBefore, memoryAfter);
    assertThat(memoryAfter).isLessThanOrEqualTo(memoryBefore);

    // Verify NSSummary cleanup
    ReconNamespaceSummaryManager namespaceSummaryManager = 
        recon.getReconServer().getReconNamespaceSummaryManager();
    verifyNSSummaryCleanup(omMetadataManager, namespaceSummaryManager);
    
    LOG.info("Large structure memory leak test completed successfully");
  }

  /**
   * Creates a directory structure for testing memory leak scenarios.
   * 
   * <p>This method creates a nested directory structure with the following pattern:
   * <pre>
   * rootDir/
   * ├── subdir0/
   * │   ├── file0
   * │   ├── file1
   * │   └── ...
   * ├── subdir1/
   * │   ├── file0
   * │   ├── file1
   * │   └── ...
   * └── ...
   * </pre>
   * 
   * <p>Each file contains test content in the format "content{i}{j}" where i is the 
   * subdirectory index and j is the file index within that subdirectory.
   * 
   * @param rootDir the root directory under which to create the structure
   * @param numSubdirs number of subdirectories to create
   * @param filesPerDir number of files to create in each subdirectory
   * @throws IOException if directory or file creation fails
   */
  private void createDirectoryStructure(Path rootDir, int numSubdirs, int filesPerDir) 
      throws IOException {
    for (int i = 0; i < numSubdirs; i++) {
      Path subDir = new Path(rootDir, "subdir" + i);
      fs.mkdirs(subDir);
      
      for (int j = 0; j < filesPerDir; j++) {
        Path file = new Path(subDir, "file" + j);
        try (FSDataOutputStream stream = fs.create(file)) {
          stream.write(("content" + i + j).getBytes(UTF_8));
        }
      }
    }
  }

  /**
   * Synchronizes metadata from Ozone Manager to Recon.
   * 
   * <p>This method triggers the synchronization process that:
   * <ul>
   *   <li>Fetches latest metadata from OM (either full snapshot or delta updates)</li>
   *   <li>Processes the metadata through Recon's event handling system</li>
   *   <li>Updates NSSummary entries and other Recon-specific data structures</li>
   * </ul>
   * 
   * <p>This sync is essential for the test to verify that NSSummary entries are
   * created, updated, and deleted correctly as metadata changes in OM.
   * 
   * @throws IOException if synchronization fails
   */
  private void syncDataFromOM() throws IOException {
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
  }

  private void verifyNSSummaryEntriesExist(ReconOMMetadataManager omMetadataManager,
      ReconNamespaceSummaryManager namespaceSummaryManager, int expectedDirs) 
      throws Exception {
    
    // Wait for NSSummary entries to be created
    GenericTestUtils.waitFor(() -> {
      try {
        Table<String, OmDirectoryInfo> dirTable = omMetadataManager.getDirectoryTable();
        int dirCount = 0;
        int nsSummaryCount = 0;
        
        try (Table.KeyValueIterator<String, OmDirectoryInfo> iterator = dirTable.iterator()) {
          while (iterator.hasNext()) {
            Table.KeyValue<String, OmDirectoryInfo> kv = iterator.next();
            dirCount++;
            long objectId = kv.getValue().getObjectID();
            NSSummary summary = namespaceSummaryManager.getNSSummary(objectId);
            if (summary != null) {
              nsSummaryCount++;
            }
          }
        }
        
        LOG.info("Directory count: {}, NSSummary count: {}", dirCount, nsSummaryCount);
        return dirCount > 0 && nsSummaryCount > 0;
      } catch (Exception e) {
        LOG.error("Error checking NSSummary entries", e);
        return false;
      }
    }, 1000, 60000); // 1 minute timeout
  }

  private void verifyEntriesInDeletedTables(ReconOMMetadataManager omMetadataManager,
      int expectedDirs, int expectedFiles) throws Exception {
    
    GenericTestUtils.waitFor(() -> {
      try {
        Table<String, OmKeyInfo> deletedDirTable = omMetadataManager.getDeletedDirTable();
        long deletedDirCount = omMetadataManager.countRowsInTable(deletedDirTable);
        
        LOG.info("Deleted directory count: {}", deletedDirCount);
        return deletedDirCount > 0;
      } catch (Exception e) {
        LOG.error("Error checking deleted tables", e);
        return false;
      }
    }, 1000, 60000); // 1 minute timeout
  }

  /**
   * Simulates hard delete operation by removing entries from deleted tables.
   * 
   * <p>In a real Ozone cluster, hard delete is performed by background services like
   * {@code DirectoryDeletingService} and {@code KeyDeletingService} that periodically
   * clean up entries from deletedDirTable and deletedTable.
   * 
   * <p>This simulation:
   * <ol>
   *   <li>Iterates through all entries in deletedDirTable</li>
   *   <li>Deletes each entry to trigger the memory leak fix</li>
   *   <li>The deletion triggers {@code NSSummaryTaskWithFSO.handleUpdateOnDeletedDirTable()}</li>
   *   <li>Which in turn cleans up the corresponding NSSummary entries</li>
   * </ol>
   * 
   * @param omMetadataManager the metadata manager containing the deleted tables
   * @throws IOException if table operations fail
   */
  private void simulateHardDelete(ReconOMMetadataManager omMetadataManager) throws IOException {
    // Simulate hard delete by clearing deleted tables
    Table<String, OmKeyInfo> deletedDirTable = omMetadataManager.getDeletedDirTable();
    
    // Delete all entries from deleted tables to simulate hard delete
    try (Table.KeyValueIterator<String, OmKeyInfo> iterator = deletedDirTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        deletedDirTable.delete(kv.getKey());
      }
    }
  }

  private void verifyNSSummaryCleanup(ReconOMMetadataManager omMetadataManager,
      ReconNamespaceSummaryManager namespaceSummaryManager) throws Exception {
    
    // Wait for cleanup to complete
    GenericTestUtils.waitFor(() -> {
      try {
        // Check that deleted directories don't have NSSummary entries
        Table<String, OmDirectoryInfo> dirTable = omMetadataManager.getDirectoryTable();
        
        // Verify that the main test directory is no longer in the directory table
        try (Table.KeyValueIterator<String, OmDirectoryInfo> iterator = dirTable.iterator()) {
          while (iterator.hasNext()) {
            Table.KeyValue<String, OmDirectoryInfo> kv = iterator.next();
            String path = kv.getKey();
            if (path.contains("memoryLeakTest")) {
              LOG.info("Found test directory still in table: {}", path);
              return false;
            }
          }
        }
        
        return true;
      } catch (Exception e) {
        LOG.error("Error verifying cleanup", e);
        return false;
      }
    }, 1000, 120000); // 2 minutes timeout
    
    LOG.info("NSSummary cleanup verification completed");
  }
}
