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

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.hadoop.ozone.recon.ReconControllerModule.ReconDaoBindingModule.RECON_DAO_LIST;
import static org.apache.ozone.recon.schema.SqlDbUtils.SQLITE_DRIVER_CLASS;
import static org.apache.ozone.recon.schema.generated.Tables.RECON_TASK_STATUS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Provider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.sqlite.SQLiteDataSource;

/**
 * Test Recon schema with different DBs.
 */
public class TestReconWithDifferentSqlDBs {
  @TempDir
  private static Path temporaryFolder;

  public static Stream<Object> parametersSource() throws IOException {
    return Stream.of(
        new AbstractReconSqlDBTest.DerbyDataSourceConfigurationProvider(
            Files.createTempDirectory(temporaryFolder, "JunitDerbyDB").toFile()),
        new SqliteDataSourceConfigurationProvider(Files.createTempDirectory(temporaryFolder, "JunitSQLDS").toFile()));
  }

  /**
   * Make sure schema was created correctly.
   * @throws SQLException
   */
  @ParameterizedTest
  @MethodSource("parametersSource")
  public void testSchemaSetup(Provider<DataSourceConfiguration> provider)
      throws SQLException, IOException {
    AbstractReconSqlDBTest reconSqlDB = new AbstractReconSqlDBTest(provider);
    reconSqlDB.createReconSchemaForTest(temporaryFolder);
    assertNotNull(reconSqlDB.getInjector());
    assertNotNull(reconSqlDB.getConfiguration());
    assertNotNull(reconSqlDB.getDslContext());
    assertNotNull(reconSqlDB.getConnection());
    RECON_DAO_LIST.forEach(dao -> {
      assertNotNull(reconSqlDB.getDao(dao));
    });
    ReconTaskStatusDao dao = reconSqlDB.getDao(ReconTaskStatusDao.class);
    dao.insert(new ReconTaskStatus("TestTask", 1L, 2L, 1, 0));
    assertEquals(1, dao.findAll().size());

    int numRows = reconSqlDB.getDslContext().
        delete(RECON_TASK_STATUS).execute();
    assertEquals(1, numRows);
    assertEquals(0, dao.findAll().size());
  }

  /**
   * Make sure the task status columns are read back after a restart, so they
   * are not reset when the status row is written again.
   */
  @ParameterizedTest
  @MethodSource("parametersSource")
  public void testTaskStatusPreservedOnReload(Provider<DataSourceConfiguration> provider) throws IOException {
    AbstractReconSqlDBTest reconSqlDB = new AbstractReconSqlDBTest(provider);
    reconSqlDB.createReconSchemaForTest(temporaryFolder);
    ReconTaskStatusDao dao = reconSqlDB.getDao(ReconTaskStatusDao.class);
    dao.insert(new ReconTaskStatus("TestTask", 1L, 2L, -1, 1));

    new ReconTaskStatusUpdaterManager(dao).getTaskStatusUpdater("TestTask").updateDetails();

    ReconTaskStatus taskStatus = dao.fetchOneByTaskName("TestTask");
    assertEquals(-1, taskStatus.getLastTaskRunStatus());
    assertEquals(1, taskStatus.getIsCurrentTaskRunning());
  }

  @Test
  public void testSqliteUsesWalAndBusyTimeout() throws Exception {
    DataSourceConfiguration configuration = new SqliteDataSourceConfigurationProvider(
        Files.createTempDirectory(temporaryFolder, "JunitSqliteWal").toFile()).get();
    try (Connection connection = new SqliteDataSourceProvider(configuration).get().getConnection()) {
      DSLContext dsl = DSL.using(connection);
      assertEquals("wal", String.valueOf(dsl.fetchValue("PRAGMA journal_mode")));
      assertEquals(String.valueOf(SqliteDataSourceProvider.BUSY_TIMEOUT_MS),
          String.valueOf(dsl.fetchValue("PRAGMA busy_timeout")));
    }

    // A journal mode set in the URL is kept.
    DataSourceConfiguration urlConfiguration = mock(DataSourceConfiguration.class);
    when(urlConfiguration.getJdbcUrl()).thenReturn(configuration.getJdbcUrl() + "?journal_mode=TRUNCATE");
    try (Connection connection = new SqliteDataSourceProvider(urlConfiguration).get().getConnection()) {
      assertEquals("truncate", String.valueOf(DSL.using(connection).fetchValue("PRAGMA journal_mode")));
    }
  }

  @Test
  public void testSqliteSelectedByUrlScheme() throws Exception {
    // The resolved DB dir may contain "derby"; the URL scheme still decides the provider.
    AbstractReconSqlDBTest reconSqlDB = new AbstractReconSqlDBTest(new SqliteDataSourceConfigurationProvider(
        Files.createTempDirectory(temporaryFolder, "derby-old").toFile()));
    reconSqlDB.createReconSchemaForTest(temporaryFolder);
    assertThat(reconSqlDB.getDataSource()).isInstanceOf(SQLiteDataSource.class);
  }

  /**
   * Local Sqlite datasource provider.
   */
  public static class SqliteDataSourceConfigurationProvider implements
      Provider<DataSourceConfiguration> {

    private final File tempDir;

    public SqliteDataSourceConfigurationProvider(File tempDir) {
      this.tempDir = tempDir;
    }

    @Override
    public DataSourceConfiguration get() {
      return new DataSourceConfiguration() {
        @Override
        public String getDriverClass() {
          return SQLITE_DRIVER_CLASS;
        }

        @Override
        public String getJdbcUrl() {
          return "jdbc:sqlite:" + tempDir.getAbsolutePath() +
              File.separator + "recon_sqlite.db";
        }

        @Override
        public String getUserName() {
          return null;
        }

        @Override
        public String getPassword() {
          return null;
        }

        @Override
        public boolean setAutoCommit() {
          return true;
        }

        @Override
        public long getConnectionTimeout() {
          return 10000;
        }

        @Override
        public String getSqlDialect() {
          return SQLDialect.SQLITE.toString();
        }

        @Override
        public Integer getMaxActiveConnections() {
          return 2;
        }

        @Override
        public long getMaxConnectionAge() {
          return 120;
        }

        @Override
        public long getMaxIdleConnectionAge() {
          return 120;
        }

        @Override
        public String getConnectionTestStatement() {
          return "SELECT 1";
        }

        @Override
        public long getIdleConnectionTestPeriod() {
          return 30;
        }
      };
    }
  }
}
