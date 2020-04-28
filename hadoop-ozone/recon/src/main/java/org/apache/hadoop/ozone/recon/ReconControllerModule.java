/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.scm.cli.ContainerOperationClient.newContainerRpcClient;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_AUTO_COMMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_DRIVER;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_JDBC_URL;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_PASSWORD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_USER;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_IDLE_CONNECTION_TEST_PERIOD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_ACTIVE_CONNECTIONS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_CONNECTION_AGE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_IDLE_CONNECTION_AGE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_IDLE_CONNECTION_TEST_STMT;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.recon.persistence.ContainerSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.DataSourceConfiguration;
import org.apache.hadoop.ozone.recon.persistence.JooqPersistenceModule;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOmMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerDBProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerDBServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTask;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTask;
import org.apache.hadoop.ozone.recon.tasks.ReconOmTask;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskControllerImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.ratis.protocol.ClientId;
import org.hadoop.ozone.recon.schema.tables.daos.ClusterGrowthDailyDao;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerHistoryDao;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.daos.MissingContainersDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.jooq.Configuration;
import org.jooq.DAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice controller that defines concrete bindings.
 */
public class ReconControllerModule extends AbstractModule {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconControllerModule.class);

  @Override
  protected void configure() {
    bind(OzoneConfiguration.class).toProvider(ConfigurationProvider.class);
    bind(ReconHttpServer.class).in(Singleton.class);
    bind(DBStore.class)
        .toProvider(ReconContainerDBProvider.class).in(Singleton.class);
    bind(ReconOMMetadataManager.class)
        .to(ReconOmMetadataManagerImpl.class);
    bind(OMMetadataManager.class).to(ReconOmMetadataManagerImpl.class);

    bind(ContainerSchemaManager.class).in(Singleton.class);
    bind(ContainerDBServiceProvider.class)
        .to(ContainerDBServiceProviderImpl.class).in(Singleton.class);
    bind(OzoneManagerServiceProvider.class)
        .to(OzoneManagerServiceProviderImpl.class).in(Singleton.class);
    bind(ReconUtils.class).in(Singleton.class);
    // Persistence - inject configuration provider
    install(new JooqPersistenceModule(
        getProvider(DataSourceConfiguration.class)));

    install(new ReconOmTaskBindingModule());
    install(new ReconDaoBindingModule());

    bind(ReconTaskController.class)
        .to(ReconTaskControllerImpl.class).in(Singleton.class);
    bind(StorageContainerServiceProvider.class)
        .to(StorageContainerServiceProviderImpl.class).in(Singleton.class);
    bind(OzoneStorageContainerManager.class)
        .to(ReconStorageContainerManagerFacade.class).in(Singleton.class);
  }

  static class ReconOmTaskBindingModule extends AbstractModule {
    @Override
    protected void configure() {
      Multibinder<ReconOmTask> taskBinder =
          Multibinder.newSetBinder(binder(), ReconOmTask.class);
      taskBinder.addBinding().to(ContainerKeyMapperTask.class);
      taskBinder.addBinding().to(FileSizeCountTask.class);
    }
  }

  /**
   * Class that has all the DAO bindings in Recon.
   */
  public static class ReconDaoBindingModule extends AbstractModule {
    public static final List<Class<? extends DAO>> RECON_DAO_LIST =
        ImmutableList.of(
            FileCountBySizeDao.class,
            ReconTaskStatusDao.class,
            MissingContainersDao.class,
            GlobalStatsDao.class,
            ClusterGrowthDailyDao.class,
            ContainerHistoryDao.class);

    @Override
    protected void configure() {
      RECON_DAO_LIST.forEach(aClass -> {
        try {
          bind(aClass).toConstructor(
              (Constructor) aClass.getConstructor(Configuration.class));
        } catch (NoSuchMethodException e) {
          LOG.error("Error creating DAO {} ", aClass.getSimpleName(), e);
        }
      });
    }
  }

  @Provides
  OzoneManagerProtocol getOzoneManagerProtocol(
      final OzoneConfiguration ozoneConfiguration) {
    OzoneManagerProtocol ozoneManagerClient = null;
    try {
      ClientId clientId = ClientId.randomId();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      ozoneManagerClient = new
          OzoneManagerProtocolClientSideTranslatorPB(
          ozoneConfiguration, clientId.toString(),
          ozoneConfiguration.get(OZONE_OM_INTERNAL_SERVICE_ID),
          ugi);
    } catch (IOException ioEx) {
      LOG.error("Error in provisioning OzoneManagerProtocol ", ioEx);
    }
    return ozoneManagerClient;
  }

  @Provides
  StorageContainerLocationProtocol getSCMProtocol(
      final OzoneConfiguration configuration) {
    StorageContainerLocationProtocol storageContainerLocationProtocol = null;
    try {
      storageContainerLocationProtocol = newContainerRpcClient(configuration);
    } catch (IOException e) {
      LOG.error("Error in provisioning StorageContainerLocationProtocol ", e);
    }
    return storageContainerLocationProtocol;
  }

  @Provides
  DataSourceConfiguration getDataSourceConfiguration(
      final OzoneConfiguration ozoneConfiguration) {

    return new DataSourceConfiguration() {
      @Override
      public String getDriverClass() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_DRIVER,
            "org.sqlite.JDBC");
      }

      @Override
      public String getJdbcUrl() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_JDBC_URL);
      }

      @Override
      public String getUserName() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_USER);
      }

      @Override
      public String getPassword() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_PASSWORD);
      }

      @Override
      public boolean setAutoCommit() {
        return ozoneConfiguration.getBoolean(
            OZONE_RECON_SQL_AUTO_COMMIT, false);
      }

      @Override
      public long getConnectionTimeout() {
        return ozoneConfiguration.getLong(
            OZONE_RECON_SQL_CONNECTION_TIMEOUT, 30000);
      }

      @Override
      public String getSqlDialect() {
        return JooqPersistenceModule.DEFAULT_DIALECT.toString();
      }

      @Override
      public Integer getMaxActiveConnections() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_MAX_ACTIVE_CONNECTIONS, 10);
      }

      @Override
      public Integer getMaxConnectionAge() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_MAX_CONNECTION_AGE, 1800);
      }

      @Override
      public Integer getMaxIdleConnectionAge() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_MAX_IDLE_CONNECTION_AGE, 3600);
      }

      @Override
      public String getConnectionTestStatement() {
        return ozoneConfiguration.get(
            OZONE_RECON_SQL_MAX_IDLE_CONNECTION_TEST_STMT, "SELECT 1");
      }

      @Override
      public Integer getIdleConnectionTestPeriod() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_IDLE_CONNECTION_TEST_PERIOD, 60);
      }
    };

  }
}
