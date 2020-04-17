package org.apache.hadoop.ozone.recon.persistence;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.ozone.recon.ReconControllerModule.ReconDaoBindingModule.RECON_DAO_LIST;
import static org.hadoop.ozone.recon.codegen.SqlDbUtils.SQLITE_DRIVER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.jooq.SQLDialect;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.inject.Provider;

/**
 * Test Recon schema with different DBs.
 */
@RunWith(Parameterized.class)
public class TestReconWithDifferentSqlDBs extends AbstractReconSqlDBTest {

  public TestReconWithDifferentSqlDBs(
      Provider<DataSourceConfiguration> provider) {
    super(provider);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> parameters() throws IOException {
    TemporaryFolder temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    return Stream.of(
        new DerbyDataSourceConfigurationProvider(temporaryFolder.newFolder()),
        new SqliteDataSourceConfigurationProvider(temporaryFolder.newFolder()))
        .map(each -> new Object[] {each})
        .collect(toList());
  }

  /**
   * Make sure schema was created correctly.
   * @throws SQLException
   */
  @Test
  public void testSchemaSetup() throws SQLException {
    assertNotNull(getInjector());
    assertNotNull(getConfiguration());
    assertNotNull(getDslContext());
    assertNotNull(getConnection());
    RECON_DAO_LIST.forEach(dao -> {
      assertNotNull(getDao(dao));
    });
    ReconTaskStatusDao dao = getDao(ReconTaskStatusDao.class);
    dao.insert(new ReconTaskStatus("TestTask", 1L, 2L));
    assertEquals(1, dao.findAll().size());
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
        public Integer getMaxConnectionAge() {
          return 120;
        }

        @Override
        public Integer getMaxIdleConnectionAge() {
          return 120;
        }

        @Override
        public String getConnectionTestStatement() {
          return "SELECT 1";
        }

        @Override
        public Integer getIdleConnectionTestPeriod() {
          return 30;
        }
      };
    }
  }
}
