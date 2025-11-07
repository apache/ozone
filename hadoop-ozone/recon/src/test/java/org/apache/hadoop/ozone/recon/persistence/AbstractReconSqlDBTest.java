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

import static org.apache.ozone.recon.schema.SqlDbUtils.DERBY_DRIVER_CLASS;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconControllerModule.ReconDaoBindingModule;
import org.apache.hadoop.ozone.recon.ReconSchemaManager;
import org.apache.ozone.recon.schema.ReconSchemaGenerationModule;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.FileSystemUtils;

/**
 * Class that provides a Recon SQL DB with all the tables created, and APIs
 * to access the DAOs easily.
 */
public class AbstractReconSqlDBTest {
  private Injector injector;
  private DSLContext dslContext;
  private Provider<DataSourceConfiguration> configurationProvider;

  public AbstractReconSqlDBTest() {
  }

  public void init(Path temporaryFolder) {
    try {
      FileSystemUtils.deleteRecursively(temporaryFolder.resolve("Config"));
      configurationProvider =
          new DerbyDataSourceConfigurationProvider(Files.createDirectory(
              temporaryFolder.resolve("Config")).toFile());
    } catch (IOException e) {
      fail();
    }
  }

  public AbstractReconSqlDBTest(Path temporaryFolder) {
    init(temporaryFolder);
  }

  protected AbstractReconSqlDBTest(Provider<DataSourceConfiguration> provider) {
    configurationProvider = provider;
  }

  @BeforeEach
  public void createReconSchemaForTest(@TempDir Path temporaryFolder) throws IOException {
    init(temporaryFolder);
    injector = Guice.createInjector(getReconSqlDBModules());
    dslContext = DSL.using(new DefaultConfiguration().set(
        injector.getInstance(DataSource.class)));
    createSchema(injector);
  }

  /**
   * Get set of Guice modules needed to setup a Recon SQL DB.
   * @return List of modules.
   */
  public List<Module> getReconSqlDBModules() {
    List<Module> modules = new ArrayList<>();
    modules.add(new JooqPersistenceModule(configurationProvider));
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(DataSourceConfiguration.class).toProvider(configurationProvider);
        bind(ReconSchemaManager.class);
      }
    });
    modules.add(new ReconSchemaGenerationModule());
    modules.add(new ReconDaoBindingModule());
    return modules;
  }

  /**
   * Method to create Recon SQL schema. Used externally from ReconTestInjector.
   * @param inj injector
   */
  public void createSchema(Injector inj) {
    ReconSchemaManager reconSchemaManager =
        inj.getInstance(ReconSchemaManager.class);
    reconSchemaManager.createReconSchema();
  }

  protected Injector getInjector() {
    return injector;
  }

  protected Connection getConnection() throws SQLException {
    return injector.getInstance(DataSource.class).getConnection();
  }

  protected DataSource getDataSource() {
    return injector.getInstance(DataSource.class);
  }

  protected DSLContext getDslContext() {
    return dslContext;
  }

  protected Configuration getConfiguration() {
    return injector.getInstance(Configuration.class);
  }

  /**
   * Get DAO of a specific type.
   * @param type DAO class type.
   * @param <T> Dao type.
   * @return Dao instance.
   */
  protected <T> T getDao(Class<T> type) {
    return injector.getInstance(type);
  }

  /**
   * Get Schema definition of a specific type. (Essentially same as last
   * method. Just with a different name for easy understanding.)
   * @param type Schema definition class type.
   * @param <T> Schema definition type.
   * @return Schema definition instance.
   */
  protected <T> T getSchemaDefinition(Class<T> type) {
    return injector.getInstance(type);
  }

  /**
   * Local Derby datasource provider.
   */
  public static class DerbyDataSourceConfigurationProvider implements
      Provider<DataSourceConfiguration> {

    private final File tempDir;

    public DerbyDataSourceConfigurationProvider(File tempDir) {
      this.tempDir = tempDir;
    }

    @Override
    public DataSourceConfiguration get() {
      return new DataSourceConfiguration() {
        @Override
        public String getDriverClass() {
          return DERBY_DRIVER_CLASS;
        }

        @Override
        public String getJdbcUrl() {
          return "jdbc:derby:" + tempDir.getAbsolutePath() +
              File.separator + "derby_recon.db;create=true";
        }

        @Override
        public String getUserName() {
          return "RECON";
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
          return SQLDialect.DERBY.toString();
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
