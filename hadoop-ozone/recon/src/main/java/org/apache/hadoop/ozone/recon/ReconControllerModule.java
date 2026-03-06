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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.scm.cli.ContainerOperationClient.newContainerRpcClient;
import static org.apache.hadoop.ozone.OmUtils.getOzoneManagerServiceId;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.recon.heatmap.HeatMapServiceImpl;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.hadoop.ozone.recon.persistence.DataSourceConfiguration;
import org.apache.hadoop.ozone.recon.persistence.JooqPersistenceModule;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOmMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ReconFileMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconGlobalStatsManagerImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTaskFSO;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTaskOBS;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTaskFSO;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountTaskOBS;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTask;
import org.apache.hadoop.ozone.recon.tasks.OmTableInsightTask;
import org.apache.hadoop.ozone.recon.tasks.ReconOmTask;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskControllerImpl;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.recon.schema.generated.tables.daos.ClusterGrowthDailyDao;
import org.apache.ozone.recon.schema.generated.tables.daos.ContainerCountBySizeDao;
import org.apache.ozone.recon.schema.generated.tables.daos.FileCountBySizeDao;
import org.apache.ozone.recon.schema.generated.tables.daos.GlobalStatsDao;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersDao;
import org.apache.ratis.protocol.ClientId;
import org.jooq.Configuration;
import org.jooq.DAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice controller that defines concrete bindings.
 */
public class ReconControllerModule extends AbstractModule {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconControllerModule.class);

  private final ReconServer reconServer;

  public ReconControllerModule(ReconServer reconServer) {
    this.reconServer = reconServer;
  }

  @Override
  protected void configure() {
    bind(ReconServer.class).toInstance(reconServer);
    bind(OzoneConfiguration.class).toProvider(ConfigurationProvider.class);
    bind(ReconHttpServer.class).in(Singleton.class);
    bind(ReconStorageConfig.class).in(Singleton.class);
    bind(ReconDBProvider.class).in(Singleton.class);
    bind(ReconOMMetadataManager.class)
        .to(ReconOmMetadataManagerImpl.class);
    bind(OMMetadataManager.class).to(ReconOmMetadataManagerImpl.class);

    bind(ContainerHealthSchemaManagerV2.class).in(Singleton.class);
    bind(ReconContainerMetadataManager.class)
        .to(ReconContainerMetadataManagerImpl.class).in(Singleton.class);
    bind(ReconFileMetadataManager.class)
        .to(ReconFileMetadataManagerImpl.class).in(Singleton.class);
    bind(ReconGlobalStatsManager.class)
        .to(ReconGlobalStatsManagerImpl.class).in(Singleton.class);
    bind(ReconNamespaceSummaryManager.class)
        .to(ReconNamespaceSummaryManagerImpl.class).in(Singleton.class);
    bind(OzoneManagerServiceProvider.class)
        .to(OzoneManagerServiceProviderImpl.class).in(Singleton.class);
    bind(ReconUtils.class).in(Singleton.class);
    bind(ReconContext.class).in(Singleton.class);
    // Persistence - inject configuration provider
    install(new JooqPersistenceModule(
        getProvider(DataSourceConfiguration.class)));

    install(new ReconOmTaskBindingModule());
    install(new ReconDaoBindingModule());
    bind(ReconTaskStatusUpdaterManager.class).in(Singleton.class);

    bind(ReconTaskController.class)
        .to(ReconTaskControllerImpl.class).in(Singleton.class);
    bind(StorageContainerServiceProvider.class)
        .to(StorageContainerServiceProviderImpl.class).in(Singleton.class);
    bind(OzoneStorageContainerManager.class)
        .to(ReconStorageContainerManagerFacade.class).in(Singleton.class);
    bind(MetricsServiceProviderFactory.class).in(Singleton.class);

    bind(HeatMapServiceImpl.class).in(Singleton.class);
  }

  static class ReconOmTaskBindingModule extends AbstractModule {
    @Override
    protected void configure() {
      Multibinder<ReconOmTask> taskBinder =
          Multibinder.newSetBinder(binder(), ReconOmTask.class);
      taskBinder.addBinding().to(ContainerKeyMapperTaskFSO.class);
      taskBinder.addBinding().to(ContainerKeyMapperTaskOBS.class);
      taskBinder.addBinding().to(FileSizeCountTaskFSO.class);
      taskBinder.addBinding().to(FileSizeCountTaskOBS.class);
      taskBinder.addBinding().to(OmTableInsightTask.class);
      taskBinder.addBinding().to(NSSummaryTask.class);
    }
  }

  @Provides
  @Singleton
  public ExecutorService provideReconExecutorService() {
    return Executors.newFixedThreadPool(5);
  }

  /**
   * Class that has all the DAO bindings in Recon.
   */
  public static class ReconDaoBindingModule extends AbstractModule {
    public static final List<Class<? extends DAO>> RECON_DAO_LIST =
        ImmutableList.of(
            FileCountBySizeDao.class,
            ReconTaskStatusDao.class,
            UnhealthyContainersDao.class,
            GlobalStatsDao.class,
            ClusterGrowthDailyDao.class,
            ContainerCountBySizeDao.class
        );

    @Override
    protected void configure() {
      RECON_DAO_LIST.forEach(aClass -> {
        try {
          bind(aClass).toConstructor(
              (Constructor) aClass.getConstructor(Configuration.class))
              .in(Singleton.class);
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
      String serviceId = getOzoneManagerServiceId(ozoneConfiguration);
      OmTransport transport =
          OmTransportFactory.create(ozoneConfiguration, ugi, serviceId);
      ozoneManagerClient = new OzoneManagerProtocolClientSideTranslatorPB(
          transport, clientId.toString());
    } catch (IOException ioEx) {
      LOG.error("Error in provisioning OzoneManagerProtocol ", ioEx);
    }
    return ozoneManagerClient;
  }

  @Provides
  StorageContainerLocationProtocol getSCMProtocol(
      final OzoneConfiguration configuration) {
    StorageContainerLocationProtocol storageContainerLocationProtocol = null;
    storageContainerLocationProtocol = newContainerRpcClient(configuration);
    return storageContainerLocationProtocol;
  }

  @Provides
  DataSourceConfiguration getDataSourceConfiguration(
      final OzoneConfiguration ozoneConfiguration) {

    ReconSqlDbConfig sqlDbConfig =
        ozoneConfiguration.getObject(ReconSqlDbConfig.class);

    if (StringUtils.contains(sqlDbConfig.getJdbcUrl(), OZONE_RECON_DB_DIR)) {
      ReconUtils reconUtils = new ReconUtils();
      File reconDbDir =
          reconUtils.getReconDbDir(ozoneConfiguration, OZONE_RECON_DB_DIR);
      sqlDbConfig.setJdbcUrl(
          "jdbc:derby:" + reconDbDir.getPath() + "/ozone_recon_derby.db");
    }

    return new DataSourceConfiguration() {
      @Override
      public String getDriverClass() {
        return sqlDbConfig.getDriverClass();
      }

      @Override
      public String getJdbcUrl() {
        return sqlDbConfig.getJdbcUrl();
      }

      @Override
      public String getUserName() {
        return sqlDbConfig.getUsername();
      }

      @Override
      public String getPassword() {
        return sqlDbConfig.getPassword();
      }

      @Override
      public boolean setAutoCommit() {
        return sqlDbConfig.isAutoCommit();
      }

      @Override
      public long getConnectionTimeout() {
        return sqlDbConfig.getConnectionTimeout();
      }

      @Override
      public String getSqlDialect() {
        return sqlDbConfig.getSqlDbDialect();
      }

      @Override
      public Integer getMaxActiveConnections() {
        return sqlDbConfig.getMaxActiveConnections();
      }

      @Override
      public long getMaxConnectionAge() {
        return sqlDbConfig.getConnectionMaxAge();
      }

      @Override
      public long getMaxIdleConnectionAge() {
        return sqlDbConfig.getConnectionIdleMaxAge();
      }

      @Override
      public String getConnectionTestStatement() {
        return sqlDbConfig.getIdleTestQuery();
      }

      @Override
      public long getIdleConnectionTestPeriod() {
        return sqlDbConfig.getConnectionIdleTestPeriod();
      }
    };

  }
}
