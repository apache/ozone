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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.metrics.ReconScmContainerSyncMetrics;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ReconStorageContainerManagerFacade}.
 */
class TestReconStorageContainerManagerFacade {

  @TempDir
  private Path temporaryFolder;

  private final List<ReconScmContainerSyncMetrics> metricsToUnregister =
      new ArrayList<>();

  @AfterEach
  void tearDown() {
    metricsToUnregister.forEach(ReconScmContainerSyncMetrics::unRegister);
    metricsToUnregister.clear();
  }

  @Test
  void testScmSnapshotDbIsOpenedAtCanonicalReconPath() throws Exception {
    StorageContainerServiceProvider scmServiceProvider =
        mock(StorageContainerServiceProviderImpl.class);
    OMMetadataManager omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve("OmMetadata")).toFile());
    ReconOMMetadataManager reconOMMetadataManager =
        getTestReconOmMetadataManager(omMetadataManager,
            Files.createDirectory(temporaryFolder.resolve("ReconOmMetadata"))
                .toFile());
    ReconTestInjector injector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                scmServiceProvider)
            .build();

    ReconStorageContainerManagerFacade reconScm =
        injector.getInstance(ReconStorageContainerManagerFacade.class);
    metricsToUnregister.add(getContainerSyncMetrics(reconScm));
    OzoneConfiguration conf = injector.getInstance(OzoneConfiguration.class);
    File checkpointDir =
        temporaryFolder.resolve("scm.snapshot.db_test").toFile();
    DBStore checkpointStore = DBStoreBuilder.newBuilder(
        conf, ReconSCMDBDefinition.get(), checkpointDir).build();
    checkpointStore.close();

    DBCheckpoint checkpoint = mock(DBCheckpoint.class);
    when(checkpoint.getCheckpointLocation()).thenReturn(checkpointDir.toPath());
    when(scmServiceProvider.getSCMDBSnapshot()).thenReturn(checkpoint);

    File canonicalReconScmDb = new File(checkpointDir.getParentFile(),
        ReconSCMDBDefinition.RECON_SCM_DB_NAME);
    assertNotEquals(checkpointDir.getCanonicalFile(),
        canonicalReconScmDb.getCanonicalFile());

    reconScm.updateReconSCMDBWithNewSnapshot();

    assertEquals(canonicalReconScmDb.getCanonicalFile(),
        reconScm.getScmDBStore().getDbLocation().getCanonicalFile());
    assertTrue(canonicalReconScmDb.exists());
    assertFalse(checkpointDir.exists());
  }

  @Test
  void testTriggerSCMContainerSyncSetsSuccessStatusAndDuration()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm = createReconScmFacade();
    ReconStorageContainerSyncHelper syncHelper =
        mock(ReconStorageContainerSyncHelper.class);
    ReconScmContainerSyncMetrics metrics = getContainerSyncMetrics(reconScm);
    metrics.setScmContainerSyncDurationMs(-1L);
    setContainerSyncHelper(reconScm, syncHelper);
    when(syncHelper.syncWithSCMContainerInfo()).thenReturn(true);

    assertTrue(reconScm.triggerSCMContainerSync());

    assertEquals(ReconScmContainerSyncMetrics.SCM_CONTAINER_SYNC_STATUS_SUCCESS,
        metrics.getScmContainerSyncStatus());
    assertNotEquals(-1L, metrics.getScmContainerSyncDurationMs());
  }

  @Test
  void testTriggerSCMContainerSyncSetsFailureStatusAndDuration()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm = createReconScmFacade();
    ReconStorageContainerSyncHelper syncHelper =
        mock(ReconStorageContainerSyncHelper.class);
    ReconScmContainerSyncMetrics metrics = getContainerSyncMetrics(reconScm);
    metrics.setScmContainerSyncDurationMs(-1L);
    setContainerSyncHelper(reconScm, syncHelper);
    when(syncHelper.syncWithSCMContainerInfo()).thenReturn(false);

    assertFalse(reconScm.triggerSCMContainerSync());

    assertEquals(ReconScmContainerSyncMetrics.SCM_CONTAINER_SYNC_STATUS_FAILURE,
        metrics.getScmContainerSyncStatus());
    assertNotEquals(-1L, metrics.getScmContainerSyncDurationMs());
  }

  @Test
  void testTriggerSCMContainerSyncSetsFailureStatusAndDurationOnException()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm = createReconScmFacade();
    ReconStorageContainerSyncHelper syncHelper =
        mock(ReconStorageContainerSyncHelper.class);
    ReconScmContainerSyncMetrics metrics = getContainerSyncMetrics(reconScm);
    RuntimeException failure = new RuntimeException("sync failed");
    metrics.setScmContainerSyncDurationMs(-1L);
    setContainerSyncHelper(reconScm, syncHelper);
    when(syncHelper.syncWithSCMContainerInfo()).thenThrow(failure);

    RuntimeException thrown = assertThrows(RuntimeException.class,
        reconScm::triggerSCMContainerSync);

    assertEquals(failure, thrown);
    assertEquals(ReconScmContainerSyncMetrics.SCM_CONTAINER_SYNC_STATUS_FAILURE,
        metrics.getScmContainerSyncStatus());
    assertNotEquals(-1L, metrics.getScmContainerSyncDurationMs());
  }

  private ReconStorageContainerManagerFacade createReconScmFacade()
      throws Exception {
    StorageContainerServiceProvider scmServiceProvider =
        mock(StorageContainerServiceProviderImpl.class);
    OMMetadataManager omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve("OmMetadata")).toFile());
    ReconOMMetadataManager reconOMMetadataManager =
        getTestReconOmMetadataManager(omMetadataManager,
            Files.createDirectory(temporaryFolder.resolve("ReconOmMetadata"))
                .toFile());
    ReconTestInjector injector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                scmServiceProvider)
            .build();
    ReconStorageContainerManagerFacade reconScm =
        injector.getInstance(ReconStorageContainerManagerFacade.class);
    metricsToUnregister.add(getContainerSyncMetrics(reconScm));
    return reconScm;
  }

  private static void setContainerSyncHelper(
      ReconStorageContainerManagerFacade reconScm,
      ReconStorageContainerSyncHelper syncHelper) throws Exception {
    Field helperField = ReconStorageContainerManagerFacade.class
        .getDeclaredField("containerSyncHelper");
    helperField.setAccessible(true);
    helperField.set(reconScm, syncHelper);
  }

  private static ReconScmContainerSyncMetrics getContainerSyncMetrics(
      ReconStorageContainerManagerFacade reconScm) throws Exception {
    Field metricsField = ReconStorageContainerManagerFacade.class
        .getDeclaredField("containerSyncMetrics");
    metricsField.setAccessible(true);
    return (ReconScmContainerSyncMetrics) metricsField.get(reconScm);
  }
}
