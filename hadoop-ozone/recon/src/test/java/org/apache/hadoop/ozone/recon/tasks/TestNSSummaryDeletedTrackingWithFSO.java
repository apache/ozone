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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for NSSummary deleted tracking functionality with FSO layout.
 * Tests tracking of deleted files and directories in namespace summaries.
 *
 * This test works with the default FSO structure:
 * vol/bucket1/
 *   ├── file1
 *   └── dir1/
 *       ├── dir2/
 *       │   └── file3
 *       └── dir3/
 * vol/bucket2/
 *   ├── file2
 *   └── file4
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestNSSummaryDeletedTrackingWithFSO extends AbstractNSSummaryTaskTest {

  private NSSummaryTaskWithFSO nSSummaryTaskWithFso;

  // Answer Sets for tracking deleted items
  private static Set<Long> deletedChildDirsBucket1 = new HashSet<>();
  private static Set<Long> deletedChildDirsDir1 = new HashSet<>();

  // Additional test constants for extra files
  private static final String FILE_SIX = "file6";
  private static final String FILE_SEVEN = "file7";

  private static final long KEY_SIX_OBJECT_ID = 100L;
  private static final long KEY_SEVEN_OBJECT_ID = 101L;

  private static final long KEY_SIX_SIZE = 600L;
  private static final long KEY_SEVEN_SIZE = 700L;

  private static BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @BeforeAll
  void setUp(@TempDir File tmpDir) throws Exception {
    commonSetup(tmpDir,
        new OMConfigParameter(true,
            false,
            getBucketLayout(),
            3,
            true,
            true,
            false));

    nSSummaryTaskWithFso = new NSSummaryTaskWithFSO(
        getReconNamespaceSummaryManager(),
        getReconOMMetadataManager(),
        3);
  }

  /**
   * Nested class for testing deleted tracking during reprocess.
   */
  @Nested
  public class TestReprocessDeletedTracking {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;
    private NSSummary nsSummaryForDir1;
    private NSSummary nsSummaryForDir2;

    @BeforeEach
    public void setUp() throws IOException {
      List<NSSummary> result = commonSetUpTestReprocess(
          () -> nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager()),
          BUCKET_ONE_OBJECT_ID, BUCKET_TWO_OBJECT_ID);

      nsSummaryForBucket1 = result.get(0);
      nsSummaryForBucket2 = result.get(1);

      // Get NSSummary for existing directories
      nsSummaryForDir1 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_ONE_OBJECT_ID);
      nsSummaryForDir2 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_TWO_OBJECT_ID);
    }

    @Test
    public void testReprocessDeletedTrackingInitialState() {
      // Initially, no deleted files or directories should be tracked
      assertEquals(0, nsSummaryForBucket1.getNumOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket1.getSizeOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket1.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForBucket1.getSizeOfDeletedDirs());
      assertEquals(0, nsSummaryForBucket1.getDeletedChildDir().size());

      assertEquals(0, nsSummaryForBucket2.getNumOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket2.getSizeOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket2.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForBucket2.getSizeOfDeletedDirs());
      assertEquals(0, nsSummaryForBucket2.getDeletedChildDir().size());
    }

    @Test
    public void testReprocessDeletedTrackingForDirectories() {
      // Test that directories have proper deleted tracking initialized
      assertNotNull(nsSummaryForDir1);
      assertNotNull(nsSummaryForDir2);

      assertEquals(0, nsSummaryForDir1.getNumOfDeletedFiles());
      assertEquals(0, nsSummaryForDir1.getSizeOfDeletedFiles());
      assertEquals(0, nsSummaryForDir1.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForDir1.getSizeOfDeletedDirs());
      assertEquals(0, nsSummaryForDir1.getDeletedChildDir().size());

      assertEquals(0, nsSummaryForDir2.getNumOfDeletedFiles());
      assertEquals(0, nsSummaryForDir2.getSizeOfDeletedFiles());
      assertEquals(0, nsSummaryForDir2.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForDir2.getSizeOfDeletedDirs());
      assertEquals(0, nsSummaryForDir2.getDeletedChildDir().size());
    }

    @Test
    public void testReprocessFileCountInDirectories() {
      // Test that file counts are correct in directories
      // dir2 should have 1 file (file3)
      assertEquals(1, nsSummaryForDir2.getNumOfFiles());
      assertEquals(KEY_THREE_SIZE, nsSummaryForDir2.getSizeOfFiles());

      // dir1 should have 1 file from dir2
      assertEquals(1, nsSummaryForDir1.getNumOfFiles());
      assertEquals(KEY_THREE_SIZE, nsSummaryForDir1.getSizeOfFiles());

      // bucket1 should have 2 files (file1 + file3 from dir2)
      assertEquals(2, nsSummaryForBucket1.getNumOfFiles());
      assertEquals(KEY_ONE_SIZE + KEY_THREE_SIZE, nsSummaryForBucket1.getSizeOfFiles());
    }
  }

  /**
   * Nested class for testing deleted tracking during process events.
   */
  @Nested
  public class TestProcessDeletedTracking {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForBucket2;
    private NSSummary nsSummaryForDir1;
    private NSSummary nsSummaryForDir2;

    @BeforeEach
    public void setUp() throws IOException {
      // Add additional files for testing
      addAdditionalTestFiles();

      nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager());
      nSSummaryTaskWithFso.processWithFSO(processDeletedTrackingEventBatch(), 0);

      nsSummaryForBucket1 = getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_ONE_OBJECT_ID);
      nsSummaryForBucket2 = getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_TWO_OBJECT_ID);
      nsSummaryForDir1 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_ONE_OBJECT_ID);
      nsSummaryForDir2 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_TWO_OBJECT_ID);
    }

    private void addAdditionalTestFiles() throws IOException {
      // Add additional files under dir2 for testing
      String omKey6Key = DIR_TWO_OBJECT_ID + OM_KEY_PREFIX + FILE_SIX;
      OmKeyInfo omKey6Info = buildOmKeyInfo(VOL, BUCKET_ONE, "dir1/dir2/file6",
          FILE_SIX, KEY_SIX_OBJECT_ID, DIR_TWO_OBJECT_ID, KEY_SIX_SIZE);
      getOmMetadataManager().getKeyTable(getBucketLayout()).put(omKey6Key, omKey6Info);

      String omKey7Key = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + FILE_SEVEN;
      OmKeyInfo omKey7Info = buildOmKeyInfo(VOL, BUCKET_ONE, FILE_SEVEN,
          FILE_SEVEN, KEY_SEVEN_OBJECT_ID, BUCKET_ONE_OBJECT_ID, KEY_SEVEN_SIZE);
      getOmMetadataManager().getKeyTable(getBucketLayout()).put(omKey7Key, omKey7Info);
    }

    private OMUpdateEventBatch processDeletedTrackingEventBatch() throws IOException {
      // Delete file 1 under bucket 1
      String omDeleteFileKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + FILE_ONE;
      OmKeyInfo omDeleteFileInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, KEY_ONE, FILE_ONE,
          KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, KEY_ONE_SIZE);
      OMDBUpdateEvent deleteFileEvent = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDeleteFileKey)
          .setValue(omDeleteFileInfo)
          .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();

      // Delete file 6 under dir2
      String omDeleteFile6Key = DIR_TWO_OBJECT_ID + OM_KEY_PREFIX + FILE_SIX;
      OmKeyInfo omDeleteFile6Info = buildOmKeyInfo(
          VOL, BUCKET_ONE, "dir1/dir2/file6", FILE_SIX,
          KEY_SIX_OBJECT_ID, DIR_TWO_OBJECT_ID, KEY_SIX_SIZE);
      OMDBUpdateEvent deleteFile6Event = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDeleteFile6Key)
          .setValue(omDeleteFile6Info)
          .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();

      // Delete dir3 under dir1
      String omDeleteDir3Key = DIR_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_THREE;
      OmDirectoryInfo omDeleteDir3Info = buildOmDirInfo(DIR_THREE,
          DIR_THREE_OBJECT_ID, DIR_ONE_OBJECT_ID);
      OMDBUpdateEvent deleteDir3Event = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmDirectoryInfo>()
          .setKey(omDeleteDir3Key)
          .setValue(omDeleteDir3Info)
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .setTable(getOmMetadataManager().getDirectoryTable().getName())
          .build();

      return new OMUpdateEventBatch(Arrays.asList(
          deleteFileEvent, deleteFile6Event, deleteDir3Event
      ), 0L);
    }

    @Test
    public void testProcessDeletedFileTracking() {
      // After deleting file 1, bucket 1 should track the deleted file
      assertEquals(1, nsSummaryForBucket1.getNumOfDeletedFiles());
      assertEquals(KEY_ONE_SIZE, nsSummaryForBucket1.getSizeOfDeletedFiles());

      // After deleting file 6, dir2 should track the deleted file
      assertEquals(1, nsSummaryForDir2.getNumOfDeletedFiles());
      assertEquals(KEY_SIX_SIZE, nsSummaryForDir2.getSizeOfDeletedFiles());

      // Bucket 2 should have no deleted files
      assertEquals(0, nsSummaryForBucket2.getNumOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket2.getSizeOfDeletedFiles());
    }

    @Test
    public void testProcessDeletedDirectoryTracking() {
      // After deleting dir3, dir1 should track the deleted directory
      assertEquals(1, nsSummaryForDir1.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForDir1.getSizeOfDeletedDirs()); // Directories have 0 size

      // Check that deleted child directory is tracked
      Set<Long> deletedChildDirs = nsSummaryForDir1.getDeletedChildDir();
      assertEquals(1, deletedChildDirs.size());
      assertTrue(deletedChildDirs.contains(DIR_THREE_OBJECT_ID));
    }

    @Test
    public void testProcessDeletedTrackingForBuckets() {
      // Bucket 1 should have deleted file tracking
      assertEquals(1, nsSummaryForBucket1.getNumOfDeletedFiles());
      assertEquals(KEY_ONE_SIZE, nsSummaryForBucket1.getSizeOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket1.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForBucket1.getSizeOfDeletedDirs());

      // Bucket 2 should have no deleted items
      assertEquals(0, nsSummaryForBucket2.getNumOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket2.getSizeOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket2.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForBucket2.getSizeOfDeletedDirs());
    }

    @Test
    public void testProcessDeletedChildDirectoryTracking() {
      // Test that deleted child directories are properly tracked
      Set<Long> deletedChildDirs = nsSummaryForDir1.getDeletedChildDir();
      assertEquals(1, deletedChildDirs.size());
      assertTrue(deletedChildDirs.contains(DIR_THREE_OBJECT_ID));

      // Other directories should not have any deleted child directories
      assertEquals(0, nsSummaryForDir2.getDeletedChildDir().size());
      assertEquals(0, nsSummaryForBucket1.getDeletedChildDir().size());
      assertEquals(0, nsSummaryForBucket2.getDeletedChildDir().size());
    }

    @Test
    public void testProcessMultipleDeleteOperations() {
      // Test that multiple delete operations are properly tracked
      // Bucket 1 should have 1 deleted file
      assertEquals(1, nsSummaryForBucket1.getNumOfDeletedFiles());
      assertEquals(KEY_ONE_SIZE, nsSummaryForBucket1.getSizeOfDeletedFiles());

      // Dir2 should have 1 deleted file
      assertEquals(1, nsSummaryForDir2.getNumOfDeletedFiles());
      assertEquals(KEY_SIX_SIZE, nsSummaryForDir2.getSizeOfDeletedFiles());

      // Dir1 should have 1 deleted directory
      assertEquals(1, nsSummaryForDir1.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForDir1.getSizeOfDeletedDirs());

      // Check that deleted child directory is tracked
      Set<Long> deletedChildDirs = nsSummaryForDir1.getDeletedChildDir();
      assertEquals(1, deletedChildDirs.size());
      assertTrue(deletedChildDirs.contains(DIR_THREE_OBJECT_ID));
    }
  }

  /**
   * Nested class for testing mixed add/delete operations.
   */
  @Nested
  public class TestMixedOperations {

    private NSSummary nsSummaryForBucket1;
    private NSSummary nsSummaryForDir1;

    @BeforeEach
    public void setUp() throws IOException {
      nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager());
      nSSummaryTaskWithFso.processWithFSO(processMixedOperationsEventBatch(), 0);

      nsSummaryForBucket1 = getReconNamespaceSummaryManager()
          .getNSSummary(BUCKET_ONE_OBJECT_ID);
      nsSummaryForDir1 = getReconNamespaceSummaryManager()
          .getNSSummary(DIR_ONE_OBJECT_ID);
    }

    private OMUpdateEventBatch processMixedOperationsEventBatch() throws IOException {
      // Add new file to bucket1
      String omAddFileKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + "newfile";
      OmKeyInfo omAddFileInfo = buildOmKeyInfo(VOL, BUCKET_ONE, "newfile",
          "newfile", 200L, BUCKET_ONE_OBJECT_ID, 800L);
      OMDBUpdateEvent addFileEvent = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omAddFileKey)
          .setValue(omAddFileInfo)
          .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();

      // Delete existing file from bucket1
      String omDeleteFileKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + FILE_ONE;
      OmKeyInfo omDeleteFileInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, KEY_ONE, FILE_ONE,
          KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, KEY_ONE_SIZE);
      OMDBUpdateEvent deleteFileEvent = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDeleteFileKey)
          .setValue(omDeleteFileInfo)
          .setTable(getOmMetadataManager().getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();

      // Add new directory under bucket1
      String omAddDirKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + "newdir";
      OmDirectoryInfo omAddDirInfo = buildOmDirInfo("newdir",
          300L, BUCKET_ONE_OBJECT_ID);
      OMDBUpdateEvent addDirEvent = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmDirectoryInfo>()
          .setKey(omAddDirKey)
          .setValue(omAddDirInfo)
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .setTable(getOmMetadataManager().getDirectoryTable().getName())
          .build();

      // Delete existing directory
      String omDeleteDirKey = DIR_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_THREE;
      OmDirectoryInfo omDeleteDirInfo = buildOmDirInfo(DIR_THREE,
          DIR_THREE_OBJECT_ID, DIR_ONE_OBJECT_ID);
      OMDBUpdateEvent deleteDirEvent = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmDirectoryInfo>()
          .setKey(omDeleteDirKey)
          .setValue(omDeleteDirInfo)
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .setTable(getOmMetadataManager().getDirectoryTable().getName())
          .build();

      return new OMUpdateEventBatch(Arrays.asList(
          addFileEvent, deleteFileEvent, addDirEvent, deleteDirEvent
      ), 0L);
    }

    @Test
    public void testMixedAddDeleteFileOperations() {
      // Test that both add and delete file operations work correctly
      // Bucket1 should have 1 deleted file and 1 new file
      assertEquals(1, nsSummaryForBucket1.getNumOfDeletedFiles());
      assertEquals(KEY_ONE_SIZE, nsSummaryForBucket1.getSizeOfDeletedFiles());
    }

    @Test
    public void testMixedAddDeleteDirectoryOperations() {
      // Test that both add and delete directory operations work correctly
      // Dir1 should have 1 deleted directory
      assertEquals(1, nsSummaryForDir1.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForDir1.getSizeOfDeletedDirs());

      // Check that deleted child directory is tracked
      Set<Long> deletedChildDirs = nsSummaryForDir1.getDeletedChildDir();
      assertEquals(1, deletedChildDirs.size());
      assertTrue(deletedChildDirs.contains(DIR_THREE_OBJECT_ID));
    }

    @Test
    public void testMixedOperationsNoInterference() {
      // Test that add and delete operations don't interfere with each other
      // File operations should not affect directory tracking
      assertEquals(1, nsSummaryForBucket1.getNumOfDeletedFiles());
      assertEquals(0, nsSummaryForBucket1.getNumOfDeletedDirs());

      // Directory operations should not affect file tracking
      assertEquals(1, nsSummaryForDir1.getNumOfDeletedDirs());
      assertEquals(0, nsSummaryForDir1.getNumOfDeletedFiles());
    }
  }
}
