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

import static org.apache.ozone.recon.schema.SchemaVersionTableDefinition.SCHEMA_VERSION_TABLE_NAME;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.DataSourceConfiguration;
import org.apache.hadoop.ozone.recon.persistence.JooqPersistenceModule;
import org.apache.hadoop.ozone.upgrade.AbstractComponentVersionManagerTest;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.jooq.Configuration;
import org.jooq.CreateTableColumnStep;
import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

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
    try (DSLContext dsl = DSL.using(configuration)) {
      try (CreateTableColumnStep createTable = dsl.createTableIfNotExists(SCHEMA_VERSION_TABLE_NAME)
          .column("version_number", SQLDataType.INTEGER.nullable(false))
          .column("applied_on", SQLDataType.TIMESTAMP)) {
        createTable.execute();
      }
      dsl.deleteFrom(DSL.table(SCHEMA_VERSION_TABLE_NAME)).execute();
      if (serializedApparentVersion != ReconVersion.INITIAL_VERSION.serialize()) {
        try (InsertSetMoreStep<?> insert = dsl.insertInto(DSL.table(SCHEMA_VERSION_TABLE_NAME))
            .set(DSL.field(name("version_number")), serializedApparentVersion)
            .set(DSL.field(name("applied_on")), DSL.currentTimestamp())) {
          insert.execute();
        }
      }
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
    }

    try (ReconVersionManager versionManager = createManager(
        ReconVersion.SOFTWARE_VERSION.serialize(), new ReconUpgradeActionProvider())) {
      assertFalse(versionManager.needsFinalization());
      ReconUpgradeAction taskStatusAction = versionManager.getUpgradeActionsForTesting()
          .get(ReconVersion.TASK_STATUS_STATISTICS);
      assertInstanceOf(ReconTaskStatusTableUpgradeAction.class, taskStatusAction);
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
