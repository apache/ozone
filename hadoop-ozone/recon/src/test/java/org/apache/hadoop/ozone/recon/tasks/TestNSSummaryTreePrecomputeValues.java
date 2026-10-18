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

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for NSSummaryTaskWithFSO to verify materialized totals after reprocessing
 * on a complex directory tree structure, using custom object IDs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestNSSummaryTreePrecomputeValues extends AbstractNSSummaryTaskTest {

  // Custom constants for this specific test tree to avoid conflicts
  private static final long CUSTOM_VOL_OBJECT_ID = 5000L;
  private static final long CUSTOM_BUCKET_ONE_OBJECT_ID = 5001L;
  private static final String CUSTOM_VOL = "customVol";
  private static final String CUSTOM_BUCKET_ONE = "customBucket1";

  private static final String DIR_A = "dirA";
  private static final String DIR_B = "dirB";
  private static final String DIR_C = "dirC";
  private static final String DIR_D = "dirD";
  private static final String DIR_E = "dirE"; // New directory
  private static final long DIR_A_OBJECT_ID = 5100L;
  private static final long DIR_B_OBJECT_ID = 5101L;
  private static final long DIR_C_OBJECT_ID = 5102L;
  private static final long DIR_D_OBJECT_ID = 5103L;
  private static final long DIR_E_OBJECT_ID = 5104L; // New directory ID

  private static final String FILE_X = "fileX";
  private static final String FILE_Y = "fileY";
  private static final String FILE_Z = "fileZ";
  private static final String FILE_P = "fileP";
  private static final String FILE_Q = "fileQ";
  private static final String FILE_NEW = "fileNew"; // New file
  private static final String FILE_E1 = "fileE1"; // File in new directory

  private static final long FILE_X_OBJECT_ID = 5200L;
  private static final long FILE_Y_OBJECT_ID = 5201L;
  private static final long FILE_Z_OBJECT_ID = 5202L;
  private static final long FILE_P_OBJECT_ID = 5203L;
  private static final long FILE_Q_OBJECT_ID = 5204L;
  private static final long FILE_NEW_OBJECT_ID = 5205L; // New file ID
  private static final long FILE_E1_OBJECT_ID = 5206L; // File in new directory ID

  private static final long FILE_X_SIZE = 1000L;
  private static final long FILE_Y_SIZE = 2000L;
  private static final long FILE_Z_SIZE = 500L;
  private static final long FILE_P_SIZE = 1500L;
  private static final long FILE_Q_SIZE = 2500L;
  private static final long FILE_NEW_SIZE = 750L; // New file size
  private static final long FILE_E1_SIZE = 1200L; // File in new directory size

  private NSSummaryTaskWithFSO nSSummaryTaskWithFso;

  @BeforeAll
  void setUp(@TempDir File tmpDir) throws Exception {
    // Setup for FSO layout, which is where the materialized view applies
    commonSetup(tmpDir,
        new OMConfigParameter(true,
            false,
            BucketLayout.FILE_SYSTEM_OPTIMIZED,
            3, // Flush threshold (not critical for reprocess tests)
            true, // Override config
            true, // Enable FS Paths
            false)); // Not legacy populate

    long threshold =
        getOzoneConfiguration().getLong(ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, 3);
    nSSummaryTaskWithFso = new NSSummaryTaskWithFSO(
        getReconNamespaceSummaryManager(),
        getReconOMMetadataManager(),
        threshold, 5, 20, 2000);

    // Populate a custom complex tree structure
    populateComplexTree();
  }

  /**
   * Overrides the inherited method to initialize OM metadata manager with custom IDs.
   * This ensures our test uses unique IDs for volumes and buckets.
   */
  @Override
  protected void initializeNewOmMetadataManager(File omDbDir, BucketLayout layout) throws IOException {
    // If FSO, use the specific FSO initialization from OMMetadataManagerTestUtils
    if (layout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      setOmMetadataManager(OMMetadataManagerTestUtils.initializeNewOmMetadataManager(omDbDir));
    } else {
      // For other layouts, set up a new OmMetadataManagerImpl
      setOmConfiguration(new OzoneConfiguration());
      getOmConfiguration().set(OMConfigKeys.OZONE_OM_DB_DIRS, omDbDir.getAbsolutePath());
      getOmConfiguration().set(OmConfig.Keys.ENABLE_FILESYSTEM_PATHS, "true");
      getOmConfiguration().set(ReconServerConfigKeys.OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD, "10");

      setOmMetadataManager(new OmMetadataManagerImpl(getOmConfiguration(), null));
    }

    // Always ensure the volume and bucket are created with our CUSTOM IDs
    // regardless of the initial setup path (FSO or other layouts).
    // This makes sure our test environment is consistent.

    String volumeKey = getOmMetadataManager().getVolumeKey(CUSTOM_VOL);
    OmVolumeArgs args = OmVolumeArgs.newBuilder()
        .setObjectID(CUSTOM_VOL_OBJECT_ID) // Use custom volume ID
        .setVolume(CUSTOM_VOL)
        .setAdminName(TEST_USER)
        .setOwnerName(TEST_USER)
        .build();
    getOmMetadataManager().getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo1 = OmBucketInfo.newBuilder()
        .setVolumeName(CUSTOM_VOL)
        .setBucketName(CUSTOM_BUCKET_ONE)
        .setObjectID(CUSTOM_BUCKET_ONE_OBJECT_ID) // Use custom bucket ID
        .setBucketLayout(layout)
        .build();

    String bucketKey1 = getOmMetadataManager().getBucketKey(CUSTOM_VOL, CUSTOM_BUCKET_ONE);
    getOmMetadataManager().getBucketTable().put(bucketKey1, bucketInfo1);
  }

  /**
   * Populates a complex directory tree structure for testing materialized totals.
   * <p>
   * The tree structure will be:
   * <pre>
   * /customVol/customBucket1                        (5 files, 7500)
   * ├── dirA (ID: 5100)                             (3 files, 3500)
   * │   ├── fileX (ID: 5200, Size: 1000)            (1 file, 1000)
   * │   └── dirB (ID: 5101)                         (2 files, 2500)
   * │       ├── fileY (ID: 5201, Size: 2000)        (1 file, 2000)
   * │       └── dirC (ID: 5102)                     (1 file, 500)
   * │           └── fileZ (ID: 5202, Size: 500)     (1 file, 500)
   * ├── dirD (ID: 5103)                             (1 file, 1500)
   * │   └── fileP (ID: 5203, Size: 1500)            (1 file, 1500)
   * └── fileQ (ID: 5204, Size: 2500)                (1 file, 2500)
   * </pre>
   * <p>
   * <b>Expected totals (NumOfFiles, SizeOfFiles):</b>
   * <ul>
   *   <li>fileX: (1, 1000)</li>
   *   <li>fileY: (1, 2000)</li>
   *   <li>fileZ: (1, 500)</li>
   *   <li>fileP: (1, 1500)</li>
   *   <li>fileQ: (1, 2500)</li>
   *   <li>dirC: (1, 500) (contains fileZ)</li>
   *   <li>dirB: (2, 2500) (contains fileY, dirC) =&gt; (1 + 1, 2000 + 500)</li>
   *   <li>dirA: (3, 3500) (contains fileX, dirB) =&gt; (1 + 2, 1000 + 2500)</li>
   *   <li>dirD: (1, 1500) (contains fileP)</li>
   *   <li>customBucket1: (5, 7500) (contains dirA, dirD, fileQ)</li>
   * </ul>
   */

  private void populateComplexTree() throws IOException {
    // No need to clear tables explicitly due to custom object IDs.


    // Create directories
    writeDirToOm(getReconOMMetadataManager(), DIR_A_OBJECT_ID,
        CUSTOM_BUCKET_ONE_OBJECT_ID, CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID, DIR_A);

    writeDirToOm(getReconOMMetadataManager(), DIR_B_OBJECT_ID,
        DIR_A_OBJECT_ID, CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID, DIR_B);

    writeDirToOm(getReconOMMetadataManager(), DIR_C_OBJECT_ID,
        DIR_B_OBJECT_ID, CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID, DIR_C);

    writeDirToOm(getReconOMMetadataManager(), DIR_D_OBJECT_ID,
        CUSTOM_BUCKET_ONE_OBJECT_ID, CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID, DIR_D);

    // Create files
    writeKeyToOm(getReconOMMetadataManager(),
        FILE_X,
        CUSTOM_BUCKET_ONE,
        CUSTOM_VOL,
        FILE_X,
        FILE_X_OBJECT_ID,
        DIR_A_OBJECT_ID, // Parent is dirA
        CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID,
        FILE_X_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    writeKeyToOm(getReconOMMetadataManager(),
        FILE_Y,
        CUSTOM_BUCKET_ONE,
        CUSTOM_VOL,
        FILE_Y,
        FILE_Y_OBJECT_ID,
        DIR_B_OBJECT_ID, // Parent is dirB
        CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID,
        FILE_Y_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    writeKeyToOm(getReconOMMetadataManager(),
        FILE_Z,
        CUSTOM_BUCKET_ONE,
        CUSTOM_VOL,
        FILE_Z,
        FILE_Z_OBJECT_ID,
        DIR_C_OBJECT_ID, // Parent is dirC
        CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID,
        FILE_Z_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    writeKeyToOm(getReconOMMetadataManager(),
        FILE_P,
        CUSTOM_BUCKET_ONE,
        CUSTOM_VOL,
        FILE_P,
        FILE_P_OBJECT_ID,
        DIR_D_OBJECT_ID, // Parent is dirD
        CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID,
        FILE_P_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    writeKeyToOm(getReconOMMetadataManager(),
        FILE_Q,
        CUSTOM_BUCKET_ONE,
        CUSTOM_VOL,
        FILE_Q,
        FILE_Q_OBJECT_ID,
        CUSTOM_BUCKET_ONE_OBJECT_ID, // Parent is customBucket1 (direct file)
        CUSTOM_BUCKET_ONE_OBJECT_ID,
        CUSTOM_VOL_OBJECT_ID,
        FILE_Q_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  /**
   * Helper method to reprocess the OM DB and then process a batch of events.
   * This ensures each test starts from a consistent state.
   *
   * @param events The OMUpdateEventBatch to process.
   * @throws IOException If an I/O error occurs.
   */
  private void runProcessEvents(OMUpdateEventBatch events) throws IOException {
    // Re-initialize NSSummary table and reprocess the initial complex tree
    // to ensure a clean and consistent starting state for each test scenario.
    getReconNamespaceSummaryManager().clearNSSummaryTable();
    nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager());

    // Process the new events
    nSSummaryTaskWithFso.processWithFSO(events, 0);
  }

  private void processEventsOnly(OMUpdateEventBatch events) throws IOException {
    nSSummaryTaskWithFso.processWithFSO(events, 0);
  }

  @Test
  public void testReprocessComplexTreeTotals() throws IOException {
    // Trigger reprocess to simulate reading from OM DB and processing into NSSummary.
    // This is where the materialized totals should be computed.
    commonSetUpTestReprocess(() -> nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager()),
        CUSTOM_BUCKET_ONE_OBJECT_ID);


    // Verify totals for each directory and the bucket

    // Verify NSSummary for dirC (contains fileZ)
    NSSummary nsSummaryDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    assertNotNull(nsSummaryDirC, "NSSummary for dirC should not be null.");
    assertEquals(1, nsSummaryDirC.getNumOfFiles(), "dirC should have 1 file.");
    assertEquals(FILE_Z_SIZE, nsSummaryDirC.getSizeOfFiles(), "dirC size should be " + FILE_Z_SIZE);
    assertTrue(nsSummaryDirC.getChildDir().isEmpty(), "dirC should have no child directories.");
    assertEquals(DIR_B_OBJECT_ID, nsSummaryDirC.getParentId(), "dirC parent ID should be dirB.");


    // Verify NSSummary for dirB (contains fileY and dirC)
    // Expected: fileY (1, 2000) + dirC (1, 500) = (2, 2500)
    NSSummary nsSummaryDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    assertNotNull(nsSummaryDirB, "NSSummary for dirB should not be null.");
    assertEquals(1 + nsSummaryDirC.getNumOfFiles(), nsSummaryDirB.getNumOfFiles(), "dirB should have 2 files.");
    assertEquals(FILE_Y_SIZE + nsSummaryDirC.getSizeOfFiles(), nsSummaryDirB.getSizeOfFiles(),
        "dirB size should be " + (FILE_Y_SIZE + FILE_Z_SIZE));
    Set<Long> expectedChildrenB = new HashSet<>(Arrays.asList(DIR_C_OBJECT_ID));
    assertEquals(expectedChildrenB, nsSummaryDirB.getChildDir(), "dirB should contain dirC.");
    assertEquals(DIR_A_OBJECT_ID, nsSummaryDirB.getParentId(), "dirB parent ID should be dirA.");


    // Verify NSSummary for dirA (contains fileX and dirB)
    // Expected: fileX (1, 1000) + dirB (2, 2500) = (3, 3500)
    NSSummary nsSummaryDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    assertNotNull(nsSummaryDirA, "NSSummary for dirA should not be null.");
    assertEquals(1 + nsSummaryDirB.getNumOfFiles(), nsSummaryDirA.getNumOfFiles(), "dirA should have 3 files.");
    assertEquals(FILE_X_SIZE + nsSummaryDirB.getSizeOfFiles(), nsSummaryDirA.getSizeOfFiles(),
        "dirA size should be " + (FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE));
    Set<Long> expectedChildrenA = new HashSet<>(Arrays.asList(DIR_B_OBJECT_ID));
    assertEquals(expectedChildrenA, nsSummaryDirA.getChildDir(), "dirA should contain dirB.");
    assertEquals(CUSTOM_BUCKET_ONE_OBJECT_ID, nsSummaryDirA.getParentId(), "dirA parent ID should be customBucket1.");


    // Verify NSSummary for dirD (contains fileP)
    NSSummary nsSummaryDirD = getReconNamespaceSummaryManager().getNSSummary(DIR_D_OBJECT_ID);
    assertNotNull(nsSummaryDirD, "NSSummary for dirD should not be null.");
    assertEquals(1, nsSummaryDirD.getNumOfFiles(), "dirD should have 1 file.");
    assertEquals(FILE_P_SIZE, nsSummaryDirD.getSizeOfFiles(), "dirD size should be " + FILE_P_SIZE);
    assertTrue(nsSummaryDirD.getChildDir().isEmpty(), "dirD should have no child directories.");
    assertEquals(CUSTOM_BUCKET_ONE_OBJECT_ID, nsSummaryDirD.getParentId(), "dirD parent ID should be customBucket1.");


    // Verify NSSummary for customBucket1 (contains dirA, dirD, fileQ)
    // Expected: dirA (3, 3500) + dirD (1, 1500) + fileQ (1, 2500) = (5, 7500)
    NSSummary nsSummaryBucket1 = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);
    assertNotNull(nsSummaryBucket1, "NSSummary for customBucket1 should not be null.");
    assertEquals(nsSummaryDirA.getNumOfFiles() + nsSummaryDirD.getNumOfFiles() + 1, nsSummaryBucket1.getNumOfFiles(),
        "customBucket1 should have 5 files.");
    assertEquals(nsSummaryDirA.getSizeOfFiles() + nsSummaryDirD.getSizeOfFiles() + FILE_Q_SIZE,
        nsSummaryBucket1.getSizeOfFiles(),
        "customBucket1 size should be " + (FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE + FILE_P_SIZE + FILE_Q_SIZE));
    Set<Long> expectedChildrenBucket1 = new HashSet<>(Arrays.asList(DIR_A_OBJECT_ID, DIR_D_OBJECT_ID));
    assertEquals(expectedChildrenBucket1, nsSummaryBucket1.getChildDir(),
        "customBucket1 should contain dirA and dirD.");
    assertEquals(0L, nsSummaryBucket1.getParentId(), "customBucket1 parent ID should be 0L (root).");
  }

  @Test
  public void testAddFileEvent() throws IOException {
    // Create a new file PUT event
    OmKeyInfo newFileKeyInfo = buildOmKeyInfo(
        CUSTOM_VOL, CUSTOM_BUCKET_ONE, FILE_NEW, FILE_NEW,
        FILE_NEW_OBJECT_ID, DIR_C_OBJECT_ID, FILE_NEW_SIZE); // Add to dirC

    OMDBUpdateEvent<String, OmKeyInfo> putFileEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(newFileKeyInfo.getFileName()) // Key for OMDB is file name
        .setValue(newFileKeyInfo)
        .setTable(getReconOMMetadataManager().getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    OMUpdateEventBatch events = new OMUpdateEventBatch(Arrays.asList(putFileEvent), 0L);
    runProcessEvents(events);

    // --- Assertions after adding FILE_NEW to DIR_C ---
    // Initial state:
    // dirC: (1 file, 500 size) - fileZ
    // dirB: (2 files, 2500 size) - fileY, dirC(fileZ)
    // dirA: (3 files, 3500 size) - fileX, dirB(fileY, dirC(fileZ))
    // customBucket1: (5 files, 7500 size) - fileQ, dirA, dirD

    // Expected changes:
    // dirC: (1+1=2 files, 500+750=1250 size)
    // dirB: (2+1=3 files, 2500+750=3250 size)
    // dirA: (3+1=4 files, 3500+750=4250 size)
    // customBucket1: (5+1=6 files, 7500+750=8250 size)

    NSSummary nsSummaryDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    assertNotNull(nsSummaryDirC, "NSSummary for dirC should not be null.");
    assertEquals(2, nsSummaryDirC.getNumOfFiles(), "dirC should have 2 files after adding new file.");
    assertEquals(FILE_Z_SIZE + FILE_NEW_SIZE, nsSummaryDirC.getSizeOfFiles(), "dirC size should be updated.");

    NSSummary nsSummaryDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    assertNotNull(nsSummaryDirB, "NSSummary for dirB should not be null.");
    assertEquals(3, nsSummaryDirB.getNumOfFiles(), "dirB should have 3 files after propagation.");
    assertEquals(FILE_Y_SIZE + FILE_Z_SIZE + FILE_NEW_SIZE, nsSummaryDirB.getSizeOfFiles(),
        "dirB size should be updated.");

    NSSummary nsSummaryDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    assertNotNull(nsSummaryDirA, "NSSummary for dirA should not be null.");
    assertEquals(4, nsSummaryDirA.getNumOfFiles(), "dirA should have 4 files after propagation.");
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE + FILE_NEW_SIZE, nsSummaryDirA.getSizeOfFiles(),
        "dirA size should be updated.");

    NSSummary nsSummaryBucket1 = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);
    assertNotNull(nsSummaryBucket1, "NSSummary for customBucket1 should not be null.");
    assertEquals(6, nsSummaryBucket1.getNumOfFiles(), "customBucket1 should have 6 files after propagation.");
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE + FILE_P_SIZE + FILE_Q_SIZE + FILE_NEW_SIZE,
        nsSummaryBucket1.getSizeOfFiles(), "customBucket1 size should be updated.");
  }

  @Test
  public void testAddDirectoryEvent() throws IOException {
    // Create a new directory PUT event (empty directory)
    OmDirectoryInfo newDirInfo = buildOmDirInfo(DIR_E, DIR_E_OBJECT_ID, DIR_A_OBJECT_ID); // Add dirE under dirA

    OMDBUpdateEvent<String, OmDirectoryInfo> putDirEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmDirectoryInfo>()
        .setKey(newDirInfo.getName()) // Key for OMDB is dir name
        .setValue(newDirInfo)
        .setTable(getReconOMMetadataManager().getDirectoryTable().getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    OMUpdateEventBatch events = new OMUpdateEventBatch(Arrays.asList(putDirEvent), 0L);
    runProcessEvents(events);

    // --- Assertions after adding DIR_E to DIR_A ---
    // Initial state:
    // dirA: (3 files, 3500 size)
    // customBucket1: (5 files, 7500 size)

    // Expected changes:
    // dirE: (0 files, 0 size) - new entry
    // dirA: (3 files, 3500 size) - no change in totals, but childDir updated
    // customBucket1: (5 files, 7500 size) - no change in totals

    NSSummary nsSummaryDirE = getReconNamespaceSummaryManager().getNSSummary(DIR_E_OBJECT_ID);
    assertNotNull(nsSummaryDirE, "NSSummary for dirE should be created.");
    assertEquals(0, nsSummaryDirE.getNumOfFiles(), "dirE should have 0 files.");
    assertEquals(0, nsSummaryDirE.getSizeOfFiles(), "dirE size should be 0.");
    assertTrue(nsSummaryDirE.getChildDir().isEmpty(), "dirE should have no child directories.");
    assertEquals(DIR_A_OBJECT_ID, nsSummaryDirE.getParentId(), "dirE parent ID should be dirA.");

    NSSummary nsSummaryDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    assertNotNull(nsSummaryDirA, "NSSummary for dirA should not be null.");
    assertEquals(3, nsSummaryDirA.getNumOfFiles(), "dirA file count should not change.");
    assertEquals(3500L, nsSummaryDirA.getSizeOfFiles(), "dirA size should not change.");
    Set<Long> expectedChildrenA = new HashSet<>(Arrays.asList(DIR_B_OBJECT_ID, DIR_E_OBJECT_ID)); // dirE added
    assertEquals(expectedChildrenA, nsSummaryDirA.getChildDir(), "dirA should contain dirB and new dirE.");

    NSSummary nsSummaryBucket1 = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);
    assertNotNull(nsSummaryBucket1, "NSSummary for customBucket1 should not be null.");
    assertEquals(5, nsSummaryBucket1.getNumOfFiles(), "customBucket1 file count should not change.");
    assertEquals(7500L, nsSummaryBucket1.getSizeOfFiles(), "customBucket1 size should not change.");
  }

  @Test
  public void testAddDirectoryWithFileEvent() throws IOException {
    // Create a new directory PUT event
    OmDirectoryInfo newDirInfo =
        buildOmDirInfo(DIR_E, DIR_E_OBJECT_ID, CUSTOM_BUCKET_ONE_OBJECT_ID); // Add dirE under customBucket1

    // Create a new file PUT event under the new directory
    OmKeyInfo newFileInNewDirKeyInfo = buildOmKeyInfo(
        CUSTOM_VOL, CUSTOM_BUCKET_ONE, FILE_E1, FILE_E1,
        FILE_E1_OBJECT_ID, DIR_E_OBJECT_ID, FILE_E1_SIZE); // Add fileE1 to dirE

    OMDBUpdateEvent<String, OmDirectoryInfo> putDirEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmDirectoryInfo>()
        .setKey(newDirInfo.getName())
        .setValue(newDirInfo)
        .setTable(getReconOMMetadataManager().getDirectoryTable().getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    OMDBUpdateEvent<String, OmKeyInfo> putFileEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(newFileInNewDirKeyInfo.getFileName())
        .setValue(newFileInNewDirKeyInfo)
        .setTable(getReconOMMetadataManager().getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .build();

    // Process dir event first, then file event
    OMUpdateEventBatch events = new OMUpdateEventBatch(Arrays.asList(putDirEvent, putFileEvent), 0L);
    runProcessEvents(events);

    // --- Assertions after adding DIR_E and FILE_E1 ---
    // Initial state:
    // customBucket1: (5 files, 7500 size)

    // Expected changes:
    // dirE: (1 file, 1200 size)
    // customBucket1: (5+1=6 files, 7500+1200=8700 size)

    NSSummary nsSummaryDirE = getReconNamespaceSummaryManager().getNSSummary(DIR_E_OBJECT_ID);
    assertNotNull(nsSummaryDirE, "NSSummary for dirE should be created.");
    assertEquals(1, nsSummaryDirE.getNumOfFiles(), "dirE should have 1 file.");
    assertEquals(FILE_E1_SIZE, nsSummaryDirE.getSizeOfFiles(), "dirE size should be " + FILE_E1_SIZE);
    assertTrue(nsSummaryDirE.getChildDir().isEmpty(), "dirE should have no child directories.");
    assertEquals(CUSTOM_BUCKET_ONE_OBJECT_ID, nsSummaryDirE.getParentId(), "dirE parent ID should be customBucket1.");

    NSSummary nsSummaryBucket1 = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);
    assertNotNull(nsSummaryBucket1, "NSSummary for customBucket1 should not be null.");
    assertEquals(6, nsSummaryBucket1.getNumOfFiles(),
        "customBucket1 should have 6 files after adding new dir with file.");
    assertEquals(7500L + FILE_E1_SIZE, nsSummaryBucket1.getSizeOfFiles(), "customBucket1 size should be updated.");
    Set<Long> expectedChildrenBucket1 = new HashSet<>(Arrays.asList(DIR_A_OBJECT_ID, DIR_D_OBJECT_ID, DIR_E_OBJECT_ID));
    assertEquals(expectedChildrenBucket1, nsSummaryBucket1.getChildDir(),
        "customBucket1 should contain dirA, dirD, and new dirE.");
  }

  @Test
  public void testDeleteFileEvent() throws IOException {
    // Create a file DELETE event (delete fileX from dirA)
    OmKeyInfo deleteFileKeyInfo = buildOmKeyInfo(
        CUSTOM_VOL, CUSTOM_BUCKET_ONE, FILE_X, FILE_X,
        FILE_X_OBJECT_ID, DIR_A_OBJECT_ID, FILE_X_SIZE); // Delete fileX from dirA

    OMDBUpdateEvent<String, OmKeyInfo> deleteFileEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(deleteFileKeyInfo.getFileName())
        .setValue(deleteFileKeyInfo)
        .setTable(getReconOMMetadataManager().getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .build();

    OMUpdateEventBatch events = new OMUpdateEventBatch(Arrays.asList(deleteFileEvent), 0L);
    runProcessEvents(events);

    // --- Assertions after deleting FILE_X from DIR_A ---
    // Initial state:
    // dirA: (3 files, 3500 size) - fileX, dirB(fileY, dirC(fileZ))
    // customBucket1: (5 files, 7500 size) - fileQ, dirA, dirD

    // Expected changes:
    // dirA: (3-1=2 files, 3500-1000=2500 size)
    // customBucket1: (5-1=4 files, 7500-1000=6500 size)

    NSSummary nsSummaryDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    assertNotNull(nsSummaryDirA, "NSSummary for dirA should not be null.");
    assertEquals(2, nsSummaryDirA.getNumOfFiles(), "dirA should have 2 files after deleting fileX.");
    assertEquals(3500L - FILE_X_SIZE, nsSummaryDirA.getSizeOfFiles(), "dirA size should be updated.");

    NSSummary nsSummaryBucket1 = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);
    assertNotNull(nsSummaryBucket1, "NSSummary for customBucket1 should not be null.");
    assertEquals(4, nsSummaryBucket1.getNumOfFiles(), "customBucket1 should have 4 files after propagation.");
    assertEquals(7500L - FILE_X_SIZE, nsSummaryBucket1.getSizeOfFiles(), "customBucket1 size should be updated.");
  }

  @Test
  public void testDeleteDirectoryEvent() throws IOException {
    // Create a directory DELETE event (delete dirC from dirB)
    // dirC initially has 1 file (fileZ) and 500 size.
    OmDirectoryInfo deleteDirInfo = buildOmDirInfo(DIR_C, DIR_C_OBJECT_ID, DIR_B_OBJECT_ID);

    OMDBUpdateEvent<String, OmDirectoryInfo> deleteDirEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmDirectoryInfo>()
        .setKey(deleteDirInfo.getName())
        .setValue(deleteDirInfo)
        .setTable(getReconOMMetadataManager().getDirectoryTable().getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .build();

    OMUpdateEventBatch events = new OMUpdateEventBatch(Arrays.asList(deleteDirEvent), 0L);
    runProcessEvents(events);

    // --- Assertions after deleting DIR_C from DIR_B ---
    // Initial state:
    // dirC: (1 file, 500 size)
    // dirB: (2 files, 2500 size) - fileY, dirC(fileZ)
    // dirA: (3 files, 3500 size) - fileX, dirB(fileY, dirC(fileZ))
    // customBucket1: (5 files, 7500 size) - fileQ, dirA, dirD

    // Expected changes:
    // dirC: should be gone or parentId 0
    // dirB: (2-1=1 file, 2500-500=2000 size) - only fileY remains
    // dirA: (3-1=2 files, 3500-500=3000 size)
    // customBucket1: (5-1=4 files, 7500-500=7000 size)

    NSSummary nsSummaryDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    // After deletion, the NSSummary for the deleted directory should still exist but be "unlinked"
    // by setting its parentId to 0.
    assertNotNull(nsSummaryDirC, "NSSummary for dirC should still exist but be unlinked.");
    assertEquals(0L, nsSummaryDirC.getParentId(), "dirC parent ID should be 0 after deletion.");

    NSSummary nsSummaryDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    assertNotNull(nsSummaryDirB, "NSSummary for dirB should not be null.");
    assertEquals(1, nsSummaryDirB.getNumOfFiles(), "dirB should have 1 file after deleting dirC.");
    assertEquals(FILE_Y_SIZE, nsSummaryDirB.getSizeOfFiles(), "dirB size should be updated (only fileY remains).");
    assertTrue(nsSummaryDirB.getChildDir().isEmpty(), "dirB should no longer contain dirC.");

    NSSummary nsSummaryDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    assertNotNull(nsSummaryDirA, "NSSummary for dirA should not be null.");
    assertEquals(2, nsSummaryDirA.getNumOfFiles(), "dirA should have 2 files after propagation.");
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE, nsSummaryDirA.getSizeOfFiles(), "dirA size should be updated.");

    NSSummary nsSummaryBucket1 = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);
    assertNotNull(nsSummaryBucket1, "NSSummary for customBucket1 should not be null.");
    assertEquals(4, nsSummaryBucket1.getNumOfFiles(), "customBucket1 should have 4 files after propagation.");
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE + FILE_P_SIZE + FILE_Q_SIZE, nsSummaryBucket1.getSizeOfFiles(),
        "customBucket1 size should be updated.");
  }

  /**
   * Tests a scenario where a directory deletion event is received first,
   * followed by a file deletion event that was conceptually inside the
   * already deleted directory.
   * <p>
   * <b>Scenario:</b>
   * <ol>
   * <li>Initial state: A complex tree with /customVol/customBucket1/dirA/dirB/dirC/fileZ.</li>
   * <li>Event 1: {@code dirC} is deleted. This means {@code dirC} is unlinked from its parent
   * ({@code dirB}), and its file/size totals are decremented from its ancestors (dirB, dirA, customBucket1).
   * However, {@code dirC}'s own NSSummary object retains its original file/size counts (1 file, 500 size)
   * as it's not explicitly zeroed out upon deletion, only unlinked.</li>
   * <li>Event 2: {@code fileZ} is deleted. This event should be handled idempotently
   * as {@code fileZ}'s contribution was already removed when {@code dirC} was deleted.
   * The totals in {@code dirC} (which is now unlinked) should remain unchanged,
   * and no further changes should propagate up the tree.</li>
   * </ol>
   * @throws IOException if an I/O error occurs during OMDB operations or event processing.
   */
  @Test
  public void testDirectoryFirstDeletionScenario() throws IOException {
    // Clear NSSummary table to ensure a clean state for this specific test
    getReconNamespaceSummaryManager().clearNSSummaryTable();
    // Repopulate the main complex tree to ensure a fresh state for this test
    populateComplexTree();
    nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager());


    // Initial state of dirC, dirB, dirA, customBucket1
    NSSummary initialDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    NSSummary initialDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    NSSummary initialDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    NSSummary initialBucket = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);

    assertNotNull(initialDirC);
    assertNotNull(initialDirB);
    assertNotNull(initialDirA);
    assertNotNull(initialBucket);

    assertEquals(1, initialDirC.getNumOfFiles());
    assertEquals(FILE_Z_SIZE, initialDirC.getSizeOfFiles());
    assertEquals(2, initialDirB.getNumOfFiles()); // fileY + fileZ
    assertEquals(FILE_Y_SIZE + FILE_Z_SIZE, initialDirB.getSizeOfFiles());
    assertEquals(3, initialDirA.getNumOfFiles()); // fileX + fileY + fileZ
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE, initialDirA.getSizeOfFiles());
    assertEquals(5, initialBucket.getNumOfFiles()); // fileQ + fileP + fileX + fileY + fileZ
    assertEquals(FILE_Q_SIZE + FILE_P_SIZE + FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE, initialBucket.getSizeOfFiles());


    // Case: Directory-First Deletion Scenario
    // Event 1: dirC cascade deleted
    // This means dirC and its contents (fileZ) are conceptually removed.
    // The NSSummaryTaskDbEventHandler will decrement dirC's total from dirB, then propagate to dirA and customBucket1.
    // It will also set dirC's parentId to 0 and remove dirC from dirB's childDir.

    OmDirectoryInfo deleteDirCInfo = buildOmDirInfo(DIR_C, DIR_C_OBJECT_ID, DIR_B_OBJECT_ID);

    OMDBUpdateEvent<String, OmDirectoryInfo> deleteDirCEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmDirectoryInfo>()
        .setKey(deleteDirCInfo.getName())
        .setValue(deleteDirCInfo)
        .setTable(getReconOMMetadataManager().getDirectoryTable().getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .build();

    processEventsOnly(new OMUpdateEventBatch(Arrays.asList(deleteDirCEvent), 0L));

    // Assert state after dirC cascade deletion
    NSSummary currentDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    NSSummary currentDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    NSSummary currentDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    NSSummary currentBucket = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);

    assertNotNull(currentDirC);
    assertEquals(0L, currentDirC.getParentId(), "dirC parent ID should be 0 after deletion.");
    // Per user's clarification, these should NOT be 0. They represent the state of dirC when it was deleted.
    assertEquals(1, currentDirC.getNumOfFiles(), "dirC should still have 1 file after unlinking.");
    assertEquals(FILE_Z_SIZE, currentDirC.getSizeOfFiles(),
        "dirC size should still be " + FILE_Z_SIZE + " after unlinking.");

    assertNotNull(currentDirB);
    assertEquals(1, currentDirB.getNumOfFiles(), "dirB should have 1 file (fileY) after deleting dirC.");
    assertEquals(FILE_Y_SIZE, currentDirB.getSizeOfFiles(), "dirB size should be updated (only fileY remains).");
    assertTrue(currentDirB.getChildDir().isEmpty(), "dirB should no longer contain dirC.");

    assertNotNull(currentDirA);
    assertEquals(1 + currentDirB.getNumOfFiles(), currentDirA.getNumOfFiles(),
        "dirA should have 2 files (fileX + fileY).");
    assertEquals(FILE_X_SIZE + currentDirB.getSizeOfFiles(), currentDirA.getSizeOfFiles(),
        "dirA size should be updated.");
    Set<Long> expectedChildrenA = new HashSet<>(Arrays.asList(DIR_B_OBJECT_ID)); // dirB is still a child
    assertEquals(expectedChildrenA, currentDirA.getChildDir(), "dirA should contain only dirB.");


    assertNotNull(currentBucket);
    assertEquals(1 + currentDirA.getNumOfFiles() + 1, currentBucket.getNumOfFiles(),
        "customBucket1 should have 4 files (fileQ + fileP + fileX + fileY).");
    assertEquals(FILE_Q_SIZE + FILE_P_SIZE + currentDirA.getSizeOfFiles(), currentBucket.getSizeOfFiles(),
        "customBucket1 size should be updated.");
    Set<Long> expectedChildrenBucket1 = new HashSet<>(Arrays.asList(DIR_A_OBJECT_ID, DIR_D_OBJECT_ID));
    assertEquals(expectedChildrenBucket1, currentBucket.getChildDir(), "customBucket1 should contain dirA and dirD.");


    // Event 2: fileZ individual delete (redundant)
    // This event should be gracefully skipped because dirC (its parent) is already unlinked or its totals are 0.
    OmKeyInfo deleteFileZInfo = buildOmKeyInfo(
        CUSTOM_VOL, CUSTOM_BUCKET_ONE, FILE_Z, FILE_Z,
        FILE_Z_OBJECT_ID, DIR_C_OBJECT_ID); // Parent is dirC

    OMDBUpdateEvent<String, OmKeyInfo> deleteFileZEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(deleteFileZInfo.getFileName())
        .setValue(deleteFileZInfo)
        .setTable(getReconOMMetadataManager().getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .build();

    processEventsOnly(new OMUpdateEventBatch(Arrays.asList(deleteFileZEvent), 0L));

    // Assert that state remains unchanged (idempotent behavior)
    NSSummary finalDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    NSSummary finalDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    NSSummary finalDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    NSSummary finalBucket = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);

    assertNotNull(finalDirC);
    assertEquals(0L, finalDirC.getParentId(), "dirC parent ID should still be 0.");
    assertEquals(0, finalDirC.getNumOfFiles(), "dirC should not have 1 file.");
    assertEquals(FILE_Z_SIZE, finalDirC.getSizeOfFiles(), "dirC size should be zero now.");

    assertEquals(1, finalDirB.getNumOfFiles());
    assertEquals(FILE_Y_SIZE, finalDirB.getSizeOfFiles());

    assertEquals(2, finalDirA.getNumOfFiles());
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE, finalDirA.getSizeOfFiles());

    assertEquals(4, finalBucket.getNumOfFiles());
    assertEquals(FILE_Q_SIZE + FILE_P_SIZE + FILE_X_SIZE + FILE_Y_SIZE, finalBucket.getSizeOfFiles());
  }

  /**
   * Tests a scenario where a file deletion event is received first,
   * followed by a directory deletion event for the parent directory.
   * <p>
   * <b>Scenario:</b>
   * <ol>
   * <li>Initial state: A complex tree with /customVol/customBucket1/dirA/dirB/dirC/fileZ.</li>
   * <li>Event 1: {@code fileZ} is deleted. This decrements the file/size totals
   * from {@code dirC} and propagates up to its ancestors.</li>
   * <li>Event 2: {@code dirC} is deleted. This unlinks {@code dirC} from its parent.
   * No further changes to totals should propagate as {@code dirC}'s totals were
   * already zeroed out by the {@code fileZ} deletion.</li>
   * </ol>
   * @throws IOException if an I/O error occurs during OMDB operations or event processing.
   */
  @Test
  public void testFileFirstDeletionScenario() throws IOException {
    // Clear NSSummary table to ensure a clean state for this specific test
    getReconNamespaceSummaryManager().clearNSSummaryTable();
    // Repopulate the main complex tree to ensure a fresh state for this test
    populateComplexTree();
    nSSummaryTaskWithFso.reprocessWithFSO(getReconOMMetadataManager());

    // Initial state of dirC, dirB, dirA, customBucket1
    NSSummary initialDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    NSSummary initialDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    NSSummary initialDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    NSSummary initialBucket = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);

    assertNotNull(initialDirC);
    assertNotNull(initialDirB);
    assertNotNull(initialDirA);
    assertNotNull(initialBucket);

    assertEquals(1, initialDirC.getNumOfFiles());
    assertEquals(FILE_Z_SIZE, initialDirC.getSizeOfFiles());
    assertEquals(2, initialDirB.getNumOfFiles()); // fileY + fileZ
    assertEquals(FILE_Y_SIZE + FILE_Z_SIZE, initialDirB.getSizeOfFiles());
    assertEquals(3, initialDirA.getNumOfFiles()); // fileX + fileY + fileZ
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE, initialDirA.getSizeOfFiles());
    assertEquals(5, initialBucket.getNumOfFiles()); // fileQ + fileP + fileX + fileY + fileZ
    assertEquals(FILE_Q_SIZE + FILE_P_SIZE + FILE_X_SIZE + FILE_Y_SIZE + FILE_Z_SIZE, initialBucket.getSizeOfFiles());

    // Case: File-First Deletion Scenario
    // Event 1: fileZ deleted
    // This will decrement totals from dirC, then propagate up to dirB, dirA, and customBucket1.
    OmKeyInfo deleteFileZInfo = buildOmKeyInfo(
        CUSTOM_VOL, CUSTOM_BUCKET_ONE, FILE_Z, FILE_Z,
        FILE_Z_OBJECT_ID, DIR_C_OBJECT_ID, FILE_Z_SIZE); // Parent is dirC

    OMDBUpdateEvent<String, OmKeyInfo> deleteFileZEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmKeyInfo>()
        .setKey(deleteFileZInfo.getFileName())
        .setValue(deleteFileZInfo)
        .setTable(getReconOMMetadataManager().getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .build();

    processEventsOnly(new OMUpdateEventBatch(Arrays.asList(deleteFileZEvent), 0L));

    // Assert state after fileZ deletion
    NSSummary currentDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    NSSummary currentDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    NSSummary currentDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    NSSummary currentBucket = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);

    assertNotNull(currentDirC);
    assertEquals(0, currentDirC.getNumOfFiles(), "dirC should have 0 files after fileZ deletion.");
    assertEquals(0, currentDirC.getSizeOfFiles(), "dirC size should be 0 after fileZ deletion.");

    assertNotNull(currentDirB);
    assertEquals(1, currentDirB.getNumOfFiles(), "dirB should have 1 file (fileY) after fileZ deletion.");
    assertEquals(FILE_Y_SIZE, currentDirB.getSizeOfFiles(), "dirB size should be updated (only fileY remains).");
    Set<Long> expectedChildrenB = new HashSet<>(Arrays.asList(DIR_C_OBJECT_ID)); // dirC is still a child
    assertEquals(expectedChildrenB, currentDirB.getChildDir(), "dirB should still contain dirC.");

    assertNotNull(currentDirA);
    assertEquals(1 + currentDirB.getNumOfFiles(), currentDirA.getNumOfFiles(),
        "dirA should have 2 files (fileX + fileY).");
    assertEquals(FILE_X_SIZE + currentDirB.getSizeOfFiles(), currentDirA.getSizeOfFiles(),
        "dirA size should be updated.");

    assertNotNull(currentBucket);
    assertEquals(1 + currentDirA.getNumOfFiles() + 1, currentBucket.getNumOfFiles(),
        "customBucket1 should have 4 files (fileQ + fileP + fileX + fileY).");
    assertEquals(FILE_Q_SIZE + FILE_P_SIZE + currentDirA.getSizeOfFiles(), currentBucket.getSizeOfFiles(),
        "customBucket1 size should be updated.");


    // Event 2: dirC individual delete (redundant in terms of totals, but unlinks)
    // This event should set dirC's parentId to 0 and remove it from dirB's childDir.
    // Its own totals should remain 0 as they were already decremented by fileZ's deletion.
    OmDirectoryInfo deleteDirCInfo = buildOmDirInfo(DIR_C, DIR_C_OBJECT_ID, DIR_B_OBJECT_ID);

    OMDBUpdateEvent<String, OmDirectoryInfo> deleteDirCEvent = new OMDBUpdateEvent.
        OMUpdateEventBuilder<String, OmDirectoryInfo>()
        .setKey(deleteDirCInfo.getName())
        .setValue(deleteDirCInfo)
        .setTable(getReconOMMetadataManager().getDirectoryTable().getName())
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
        .build();

    processEventsOnly(new OMUpdateEventBatch(Arrays.asList(deleteDirCEvent), 0L));

    // Assert final state after dirC deletion
    NSSummary finalDirC = getReconNamespaceSummaryManager().getNSSummary(DIR_C_OBJECT_ID);
    NSSummary finalDirB = getReconNamespaceSummaryManager().getNSSummary(DIR_B_OBJECT_ID);
    NSSummary finalDirA = getReconNamespaceSummaryManager().getNSSummary(DIR_A_OBJECT_ID);
    NSSummary finalBucket = getReconNamespaceSummaryManager().getNSSummary(CUSTOM_BUCKET_ONE_OBJECT_ID);

    assertNotNull(finalDirC);
    assertEquals(0L, finalDirC.getParentId(), "dirC parent ID should be 0 after deletion.");
    assertEquals(0, finalDirC.getNumOfFiles(), "dirC should still have 0 files after unlinking.");
    assertEquals(0, finalDirC.getSizeOfFiles(), "dirC size should still be 0 after unlinking.");

    assertNotNull(finalDirB);
    assertEquals(1, finalDirB.getNumOfFiles(), "dirB should still have 1 file.");
    assertEquals(FILE_Y_SIZE, finalDirB.getSizeOfFiles());
    assertTrue(finalDirB.getChildDir().isEmpty(), "dirB should no longer contain dirC.");

    assertNotNull(finalDirA);
    assertEquals(2, finalDirA.getNumOfFiles());
    assertEquals(FILE_X_SIZE + FILE_Y_SIZE, finalDirA.getSizeOfFiles());

    assertNotNull(finalBucket);
    assertEquals(4, finalBucket.getNumOfFiles());
    assertEquals(FILE_Q_SIZE + FILE_P_SIZE + FILE_X_SIZE + FILE_Y_SIZE, finalBucket.getSizeOfFiles());
  }

}
