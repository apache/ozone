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

package org.apache.hadoop.ozone.recon.upgrade;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.REPLICA_HISTORY_V2;
import static org.apache.ozone.recon.schema.SchemaVersionTableDefinition.SCHEMA_VERSION_TABLE_NAME;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.DataSourceConfiguration;
import org.apache.hadoop.ozone.recon.persistence.JooqPersistenceModule;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistory;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ReconReplicaHistoryDBProvider;
import org.apache.hadoop.ozone.upgrade.AbstractComponentVersionManagerTest;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.MockedStatic;

/**
 * Tests for {@link ReconVersionManager}.
 */
class TestReconVersionManager extends AbstractComponentVersionManagerTest {

  @TempDir
  private Path tempDir;

  private static final List<ComponentVersion> ALL_VERSIONS =
      Arrays.stream(ReconVersion.values())
          .filter(v -> v != ReconVersion.UNKNOWN_VERSION)
          .collect(Collectors.toList());

  public static Stream<Arguments> preFinalizedVersionArgs() {
    return ALL_VERSIONS.stream()
        .limit(ALL_VERSIONS.size() - 1)
        .map(Arguments::of);
  }

  @Override
  protected ComponentVersionManager createManager(int serializedApparentVersion) throws IOException {
    return createManager(serializedApparentVersion, HashMap::new);
  }

  private ReconVersionManager createManager(int serializedApparentVersion,
      ComponentUpgradeActionProvider<ReconUpgradeAction> actions) throws IOException {
    try {
      Injector injector = createTestInjector("recon-version-");
      DataSource dataSource = injector.getInstance(DataSource.class);
      initializeVersionTable(injector.getInstance(Configuration.class), serializedApparentVersion);
      return new ReconVersionManager(dataSource, actions);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private Injector createTestInjector(String directoryPrefix) throws IOException {
    Path dbDir = Files.createTempDirectory(tempDir, directoryPrefix);
    File configDir = Files.createDirectory(dbDir.resolve("Config")).toFile();
    Provider<DataSourceConfiguration> configProvider =
        new AbstractReconSqlDBTest.DerbyDataSourceConfigurationProvider(configDir);
    return Guice.createInjector(
        new JooqPersistenceModule(configProvider),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DataSourceConfiguration.class).toProvider(configProvider);
          }
        });
  }

  private static void initializeVersionTable(Configuration configuration,
      int serializedApparentVersion) throws SQLException {
    DSLContext dsl = DSL.using(configuration);
    dsl.createTableIfNotExists(SCHEMA_VERSION_TABLE_NAME)
        .column("version_number", SQLDataType.INTEGER.nullable(false))
        .column("applied_on", SQLDataType.TIMESTAMP)
        .execute();
    dsl.deleteFrom(DSL.table(SCHEMA_VERSION_TABLE_NAME)).execute();
    if (serializedApparentVersion != ReconVersion.INITIAL_VERSION.serialize()) {
      dsl.insertInto(DSL.table(SCHEMA_VERSION_TABLE_NAME))
          .set(DSL.field(name("version_number")), serializedApparentVersion)
          .set(DSL.field(name("applied_on")), DSL.currentTimestamp())
          .execute();
    }
  }

  @Override
  protected List<ComponentVersion> allVersionsInOrder() {
    return ALL_VERSIONS;
  }

  @Override
  protected ComponentVersion expectedSoftwareVersion() {
    return ReconVersion.SOFTWARE_VERSION;
  }

  @Override
  @Test
  public void testClasspathScanDiscoversUpgradeActions() throws Exception {
    try (ReconVersionManager versionManager = createManager(
        ReconVersion.INITIAL_VERSION.serialize(), new ReconUpgradeActionProvider())) {
      assertTrue(versionManager.needsFinalization());
      ReconUpgradeAction taskStatusAction = versionManager.getUpgradeActionsForTesting()
          .get(ReconVersion.TASK_STATUS_STATISTICS);
      assertInstanceOf(ReconTaskStatusTableUpgradeAction.class, taskStatusAction);
      assertInstanceOf(ReplicaHistoryDBUpgradeAction.class,
          versionManager.getUpgradeActionsForTesting().get(ReconVersion.REPLICA_HISTORY_DB));
    }

    try (ReconVersionManager versionManager = createManager(
        ReconVersion.SOFTWARE_VERSION.serialize(), new ReconUpgradeActionProvider())) {
      assertFalse(versionManager.needsFinalization());
      ReconUpgradeAction taskStatusAction = versionManager.getUpgradeActionsForTesting()
          .get(ReconVersion.TASK_STATUS_STATISTICS);
      assertInstanceOf(ReconTaskStatusTableUpgradeAction.class, taskStatusAction);
      assertInstanceOf(ReplicaHistoryDBUpgradeAction.class,
          versionManager.getUpgradeActionsForTesting().get(ReconVersion.REPLICA_HISTORY_DB));
    }
  }

  @Override
  @Test
  public void testFinalizeRunsSuppliedUpgradeAction() throws Exception {
    ReconUpgradeAction mockReplicaMismatchAction = mock(ReconUpgradeAction.class);
    ReconUpgradeAction mockContainerIdIndexAction = mock(ReconUpgradeAction.class);

    ComponentUpgradeActionProvider<ReconUpgradeAction> provider = () -> {
      Map<ComponentVersion, ReconUpgradeAction> m = new HashMap<>();
      m.put(ReconVersion.UNHEALTHY_CONTAINER_REPLICA_MISMATCH, mockReplicaMismatchAction);
      m.put(ReconVersion.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX, mockContainerIdIndexAction);
      return m;
    };

    try (ReconVersionManager versionManager = createManager(
        ReconVersion.UNHEALTHY_CONTAINER_REPLICA_MISMATCH.serialize(), provider)) {
      versionManager.finalizeUpgrade();
      assertEquals(ReconVersion.SOFTWARE_VERSION, versionManager.getApparentVersion());

      // Apparent version is already UNHEALTHY_CONTAINER_REPLICA_MISMATCH; finalization runs actions for later
      // versions only, not for UNHEALTHY_CONTAINER_REPLICA_MISMATCH itself.
      verify(mockReplicaMismatchAction, never()).execute(any());
      verify(mockContainerIdIndexAction, atLeastOnce()).execute(any());
      assertEquals(ReconVersion.SOFTWARE_VERSION.serialize(),
          versionManager.getPersistedApparentVersion());
    }
  }

  @Test
  void testReplicaHistoryMigrationBeforeVersionFinalization() throws Exception {
    Injector reconInjector = createReplicaHistoryInjector();
    try (DBStore legacyStore = reconInjector.getInstance(ReconDBProvider.class).getDbStore();
         ReconReplicaHistoryDBProvider historyProvider = reconInjector.getInstance(ReconReplicaHistoryDBProvider.class);
         ReconVersionManager versionManager = createManager(
             ReconVersion.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX.serialize(), new ReconUpgradeActionProvider());
         MockedStatic<ReconGuiceServletContextListener> listener = mockStatic(ReconGuiceServletContextListener.class)) {
      ContainerReplicaHistoryList history = legacyReplicaHistory();
      REPLICA_HISTORY_V2.getTable(legacyStore).put(1L, history);
      assertTrue(historyProvider.getReplicaHistoryTable().isEmpty());
      listener.when(ReconGuiceServletContextListener::getGlobalInjector).thenReturn(reconInjector);

      versionManager.finalizeUpgrade();

      assertEquals(history.toProto(), historyProvider.getReplicaHistoryTable().get(1L).toProto());
      assertEquals(history.toProto(), REPLICA_HISTORY_V2.getTable(legacyStore).get(1L).toProto());
      assertEquals(ReconVersion.REPLICA_HISTORY_DB, versionManager.getApparentVersion());
      assertEquals(ReconVersion.REPLICA_HISTORY_DB.serialize(), versionManager.getPersistedApparentVersion());
    }
  }

  @Test
  void testReplicaHistoryMigrationFailureDoesNotAdvanceVersion() throws Exception {
    Injector reconInjector = mock(Injector.class);
    ReconDBProvider sourceProvider = mock(ReconDBProvider.class);
    when(sourceProvider.getDbStore()).thenReturn(mock(DBStore.class));
    ReconReplicaHistoryDBProvider historyProvider = mock(ReconReplicaHistoryDBProvider.class);
    when(reconInjector.getInstance(ReconDBProvider.class)).thenReturn(sourceProvider);
    when(reconInjector.getInstance(ReconReplicaHistoryDBProvider.class)).thenReturn(historyProvider);
    doThrow(new IOException("Interrupted replica history migration")).doNothing()
        .when(historyProvider).migrateReplicaHistory(any());

    try (ReconVersionManager versionManager = createManager(
             ReconVersion.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX.serialize(), new ReconUpgradeActionProvider());
         MockedStatic<ReconGuiceServletContextListener> listener = mockStatic(ReconGuiceServletContextListener.class)) {
      listener.when(ReconGuiceServletContextListener::getGlobalInjector).thenReturn(reconInjector);
      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED, thrown.getResult());
      assertEquals(ReconVersion.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX, versionManager.getApparentVersion());
      assertEquals(ReconVersion.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX.serialize(),
          versionManager.getPersistedApparentVersion());

      versionManager.finalizeUpgrade();
      assertEquals(ReconVersion.REPLICA_HISTORY_DB.serialize(), versionManager.getPersistedApparentVersion());
      verify(historyProvider, times(2)).migrateReplicaHistory(any());
    }
  }

  @Test
  void testReplicaHistoryMigrationNotRepeatedAfterVersionPersistenceFailure() throws Exception {
    Injector reconInjector = createReplicaHistoryInjector();
    ReconDBProvider sourceProvider = spy(reconInjector.getInstance(ReconDBProvider.class));
    DBStore sourceStore = spy(sourceProvider.getDbStore());
    when(sourceProvider.getDbStore()).thenReturn(sourceStore);
    Injector migrationInjector = mock(Injector.class);
    when(migrationInjector.getInstance(ReconDBProvider.class)).thenReturn(sourceProvider);

    try (DBStore legacyStore = sourceStore;
         ReconReplicaHistoryDBProvider historyProvider = reconInjector.getInstance(ReconReplicaHistoryDBProvider.class);
         ReconVersionManager versionManager = spy(createManager(
             ReconVersion.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX.serialize(), new ReconUpgradeActionProvider()));
         MockedStatic<ReconGuiceServletContextListener> listener = mockStatic(ReconGuiceServletContextListener.class)) {
      when(migrationInjector.getInstance(ReconReplicaHistoryDBProvider.class)).thenReturn(historyProvider);
      listener.when(ReconGuiceServletContextListener::getGlobalInjector).thenReturn(migrationInjector);
      ContainerReplicaHistoryList history = legacyReplicaHistory();
      REPLICA_HISTORY_V2.getTable(legacyStore).put(1L, history);
      doThrow(new IOException("Version persistence failed")).doCallRealMethod()
          .when(versionManager).persistApparentVersion(ReconVersion.REPLICA_HISTORY_DB);

      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.APPARENT_VERSION_UPDATE_FAILED, thrown.getResult());
      assertEquals(history.toProto(), historyProvider.getReplicaHistoryTable().get(1L).toProto());
      assertEquals(ReconVersion.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX.serialize(),
          versionManager.getPersistedApparentVersion());

      clearInvocations(sourceStore);
      versionManager.finalizeUpgrade();
      verifyNoInteractions(sourceStore);
      assertEquals(ReconVersion.REPLICA_HISTORY_DB.serialize(), versionManager.getPersistedApparentVersion());
    }
  }

  private Injector createReplicaHistoryInjector() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_DB_DIR, Files.createDirectory(tempDir.resolve("replica-history")).toString());
    return Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(OzoneConfiguration.class).toInstance(configuration);
        bind(ReconDBProvider.class).in(Singleton.class);
        bind(ReconReplicaHistoryDBProvider.class).in(Singleton.class);
      }
    });
  }

  private static ContainerReplicaHistoryList legacyReplicaHistory() {
    return new ContainerReplicaHistoryList(Collections.singletonList(new ContainerReplicaHistory(
        DatanodeID.randomID(), 10L, 20L, 30L, "CLOSED", ContainerChecksums.of(1234L, 0L))));
  }

  @Override
  @Test
  public void testUpgradeActionFailureAbortsFinalize() throws Exception {
    ComponentUpgradeActionProvider<ReconUpgradeAction> provider = () -> {
      Map<ComponentVersion, ReconUpgradeAction> m = new HashMap<>();
      m.put(ReconVersion.TASK_STATUS_STATISTICS, ds -> {
        throw new IOException("expected test failure");
      });
      return m;
    };

    try (ReconVersionManager versionManager = createManager(
        ReconVersion.INITIAL_VERSION.serialize(), provider)) {
      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED, thrown.getResult());
      assertEquals(ReconVersion.INITIAL_VERSION, versionManager.getApparentVersion());
      assertEquals(ReconVersion.INITIAL_VERSION.serialize(),
          versionManager.getPersistedApparentVersion());
    }
  }

  @Test
  public void testGetVersionForClientReturnsApparentVersion() throws Exception {
    // Recon has no separate ZDU/layout-feature split, so the client version is always the apparent version.
    try (ComponentVersionManager versionManager = createManager(ReconVersion.SOFTWARE_VERSION.serialize())) {
      assertEquals(ReconVersion.SOFTWARE_VERSION, versionManager.getVersionForClient());
    }

    try (ComponentVersionManager versionManager = createManager(ReconVersion.INITIAL_VERSION.serialize())) {
      assertEquals(ReconVersion.INITIAL_VERSION, versionManager.getVersionForClient());
    }
  }

  @Override
  @Test
  public void testPersistFailureRollsBack() throws Exception {
    Injector injector = createTestInjector("recon-persist-fail-");
    DataSource dataSource = injector.getInstance(DataSource.class);
    initializeVersionTable(injector.getInstance(Configuration.class),
        ReconVersion.INITIAL_VERSION.serialize());
    ReconVersionManager versionManager = spy(new ReconVersionManager(dataSource, HashMap::new));
    doThrow(new IOException("persist failed"))
        .when(versionManager).persistApparentVersion(ReconVersion.TASK_STATUS_STATISTICS);

    try (ReconVersionManager manager = versionManager) {
      assertEquals(ReconVersion.INITIAL_VERSION, manager.getApparentVersion());
      UpgradeException thrown = assertThrows(UpgradeException.class, manager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.APPARENT_VERSION_UPDATE_FAILED, thrown.getResult());
      assertEquals(ReconVersion.INITIAL_VERSION, manager.getApparentVersion());
      assertEquals(ReconVersion.INITIAL_VERSION.serialize(),
          manager.getPersistedApparentVersion());
    }
  }
}
