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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that ReconTaskControllerImpl.reInitializeTasks() correctly refreshes
 * the in-memory state of the registered task via {@link ReconOmTask#init()} after
 * the staged DB is swapped in, so that subsequent delta events use the up-to-date
 * base count from reprocess().
 *
 * <p>This test exercises the production code path through
 * {@link ReconTaskControllerImpl#reInitializeTasks} and will FAIL without the call
 * to {@code ReconOmTask.init()} on each registered task after a successful reinit.
 *
 * <p>The flow under test:
 * <ol>
 *   <li>The staged task runs {@code reprocess()}, writing count=8 to the staged DB.</li>
 *   <li>{@code replaceStagedDb()} swaps the staged DB in as the live DB.</li>
 *   <li>{@code reconGlobalStatsManager.reinitialize()} points the manager at the new DB.</li>
 *   <li>{@code init()} is called on the original (registered) task, which reloads
 *       {@code objectCountMap} from the now-updated DB (base=8).</li>
 * </ol>
 */
public class TestOmTableInsightTaskStaleCounterAfterReinit extends AbstractReconSqlDBTest {

  @TempDir
  private Path temporaryFolder;

  private ReconGlobalStatsManager reconGlobalStatsManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconTaskController reconTaskController;
  private ReconTaskStatusDao reconTaskStatusDao;

  private static final String TEST_USER = "TestUser";

  public TestOmTableInsightTaskStaleCounterAfterReinit() {
    super();
  }

  @BeforeEach
  public void setUp() throws IOException {
    // initializeNewOmMetadataManager already writes 1 volume ("sampleVol") to the OM DB.
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(
            Files.createDirectory(temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("ReconDir")).toFile());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withContainerDB()
            .build();
    reconGlobalStatsManager = reconTestInjector.getInstance(ReconGlobalStatsManager.class);

    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);

    ReconTaskStatusUpdaterManager taskStatusUpdaterManagerMock =
        mock(ReconTaskStatusUpdaterManager.class);
    when(taskStatusUpdaterManagerMock.getTaskStatusUpdater(anyString()))
        .thenAnswer(i -> new ReconTaskStatusUpdater(reconTaskStatusDao, (String) i.getArgument(0)));
    ReconDBProvider reconDbProvider = mock(ReconDBProvider.class);
    when(reconDbProvider.getDbStore()).thenReturn(mock(DBStore.class));
    when(reconDbProvider.getStagedReconDBProvider()).thenReturn(reconDbProvider);

    reconTaskController = new ReconTaskControllerImpl(
        ozoneConfiguration,
        new HashSet<>(),
        taskStatusUpdaterManagerMock,
        reconDbProvider,
        mock(ReconContainerMetadataManager.class),
        mock(ReconNamespaceSummaryManager.class),
        mock(ReconGlobalStatsManager.class),
        mock(ReconFileMetadataManager.class));
    reconTaskController.start();
  }

  /**
   * Full end-to-end validation of the reinit fix.
   *
   * <p>Timeline:
   * <pre>
   *   Phase 1 — Initial reprocess (5 volumes)
   *     originalTask.objectCountMap = {volumeTableCount: 5}
   *     globalStatsTable DB          = {volumeTableCount: 5}
   *
   *   Phase 2 — reInitializeTasks (3 more volumes added, total 8)
   *     getStagedTask() returns a new OmTableInsightTask (the "staged task")
   *     stagedTask.reprocess() → writes {volumeTableCount: 8} to the staged DB
   *     replaceStagedDb() → staged DB becomes the live DB
   *     reconGlobalStatsManager.reinitialize() → manager now reads from the new DB
   *     originalTask.init() → reloads objectCountMap from DB → {volumeTableCount: 8}  ← THE FIX
   *     globalStatsTable DB = {volumeTableCount: 8}
   *
   *   Phase 3 — delta PUT event routed through the (same) original task in reconOmTasks
   *     originalTask.objectCountMap base = 8  (refreshed by init())
   *     8 + 1 = 9  → globalStatsTable DB = {volumeTableCount: 9}  ← CORRECT
   *
   *   Without the fix (init() not called):
   *     originalTask.objectCountMap is still stale (base = 5)
   *     5 + 1 = 6 ≠ 9  →  test fails
   * </pre>
   */
  @Test
  public void testReInitializeTasksReplacesTaskReferenceAndFixesStaleCounter()
      throws IOException, InterruptedException, TimeoutException {

    //Phase 1: Initial reprocess with 5 volumes
    for (int i = 1; i <= 4; i++) {
      writeVolumeToReconOmDB("phase1-vol" + i);
    }

    OmTableInsightTask originalTask =
        spy(new OmTableInsightTask(reconGlobalStatsManager, reconOMMetadataManager));
    doAnswer(invocation ->
        new OmTableInsightTask(reconGlobalStatsManager, reconOMMetadataManager))
        .when(originalTask).getStagedTask(any(), any());

    reconTaskController.registerTask(originalTask);
    originalTask.reprocess(reconOMMetadataManager);

    GenericTestUtils.waitFor(() -> {
      try {
        return readVolumeCountFromDB() > 0;
      } catch (IOException e) {
        return false;
      }
    }, 300, 10000);
    assertEquals(5L, readVolumeCountFromDB(),
        "After Phase 1 reprocess: DB should show 5 volumes");
    // Phase 2: reInitializeTasks with 3 more volumes (total = 8)
    for (int i = 1; i <= 3; i++) {
      writeVolumeToReconOmDB("phase2-vol" + i);
    }
    // Total in OM DB: 1 (sampleVol) + 4 (phase1) + 3 (phase2) = 8
    boolean isSuccess = reconTaskController.reInitializeTasks(reconOMMetadataManager, null);
    assertTrue(isSuccess, "reInitializeTasks must succeed for the fix to be exercised");

    assertEquals(8L, readVolumeCountFromDB(),
        "After Phase 2 reinit reprocess: DB should show 8 volumes");

    ReconOmTask taskAfterReinit =
        reconTaskController.getRegisteredTasks().get("OmTableInsightTask");

    // Phase 3: delta PUT event routed through the task in reconOmTasks
    OmVolumeArgs newVol = OmVolumeArgs.newBuilder()
        .setVolume("delta-vol1")
        .setAdminName(TEST_USER)
        .setOwnerName(TEST_USER)
        .build();

    List<OMDBUpdateEvent> deltaEventList = new ArrayList<>();
    deltaEventList.add(new OMDBUpdateEvent.OMUpdateEventBuilder<>()
        .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
        .setTable(VOLUME_TABLE)
        .setKey("/delta-vol1")
        .setValue(newVol)
        .build());

    OMUpdateEventBatch deltaEventBatch = new OMUpdateEventBatch(deltaEventList, 300L);
    taskAfterReinit.process(deltaEventBatch, Collections.emptyMap());
    assertEquals(9L, readVolumeCountFromDB(),
        "Staged task (fresh base=8) + delta PUT (+1) must equal 9. "
            + "Without fix: original task's stale base (5) + delta = 6.");
  }

  /**
   * Writes a volume directly to the Recon OM metadata manager.
   */
  private void writeVolumeToReconOmDB(String volumeName) throws IOException {
    String volumeKey = reconOMMetadataManager.getVolumeKey(volumeName);
    OmVolumeArgs args = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName(TEST_USER)
        .setOwnerName(TEST_USER)
        .build();
    reconOMMetadataManager.getVolumeTable().put(volumeKey, args);
  }

  /**
   * Reads the current volumeTableCount value from the RocksDB-backed globalStatsTable.
   */
  private long readVolumeCountFromDB() throws IOException {
    String key = OmTableInsightTask.getTableCountKeyFromTable(VOLUME_TABLE);
    GlobalStatsValue value = reconGlobalStatsManager.getGlobalStatsValue(key);
    return value != null ? value.getValue() : 0L;
  }
}
