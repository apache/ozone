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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconUtils.createTarFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.common.ReconTestUtils;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for OMSyncEndpoint.
 */
public class TestTriggerDBSyncEndpoint {

  @TempDir
  private Path temporaryFolder;
  private ReconTestInjector reconTestInjector;

  @BeforeEach
  public void setUp() throws IOException, AuthenticationException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR,
        Files.createDirectory(temporaryFolder.resolve("OmSnapshotDB"))
            .toFile().getAbsolutePath());
    configuration.set(OZONE_RECON_DB_DIR,
        Files.createDirectory(temporaryFolder.resolve("ReconDb"))
            .toFile().getAbsolutePath());
    configuration.set(OZONE_OM_ADDRESS_KEY, "localhost:9862");
    OzoneManagerProtocol ozoneManagerProtocol
        = mock(OzoneManagerProtocol.class);
    when(ozoneManagerProtocol.getDBUpdates(any(OzoneManagerProtocolProtos
        .DBUpdatesRequest.class))).thenReturn(new DBUpdates());

    OMMetadataManager omMetadataManager =
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("OnMetadata")).toFile());
    ReconOMMetadataManager reconOMMetadataManager
        = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(
            temporaryFolder.resolve("OmMetadataTest")).toFile());


    ReconUtils reconUtilsMock = mock(ReconUtils.class);

    ReconTaskStatusDao reconTaskStatusDaoMock = mock(ReconTaskStatusDao.class);
    ReconTaskStatusUpdaterManager taskStatusUpdaterManagerMock = mock(ReconTaskStatusUpdaterManager.class);
    when(taskStatusUpdaterManagerMock.getTaskStatusUpdater(anyString())).thenReturn(new ReconTaskStatusUpdater(
        reconTaskStatusDaoMock, "dummyTaskManager"));

    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    File tarFile = createTarFile(checkpoint.getCheckpointLocation());
    HttpURLConnection httpURLConnectionMock = mock(HttpURLConnection.class);
    try (InputStream inputStream = Files.newInputStream(tarFile.toPath())) {
      when(httpURLConnectionMock.getInputStream()).thenReturn(inputStream);
    }
    when(reconUtilsMock.makeHttpCall(any(), anyString(), anyBoolean()))
        .thenReturn(httpURLConnectionMock);
    when(reconUtilsMock.getReconNodeDetails(
        any(OzoneConfiguration.class))).thenReturn(
        ReconTestUtils.getReconNodeDetails());

    ReconTaskController reconTaskController = mock(ReconTaskController.class);
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new OzoneManagerServiceProviderImpl(configuration,
            reconOMMetadataManager, reconTaskController,
            reconUtilsMock, ozoneManagerProtocol, new ReconContext(configuration, reconUtilsMock),
            taskStatusUpdaterManagerMock);
    ozoneManagerServiceProvider.start();

    reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(NodeEndpoint.class)
            .addBinding(MetricsServiceProviderFactory.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .addBinding(ReconUtils.class, reconUtilsMock)
            .addBinding(StorageContainerLocationProtocol.class,
                mock(StorageContainerLocationProtocol.class))
            .build();
  }

  @Test
  public void testTriggerDBSyncEndpointWithOM() {
    TriggerDBSyncEndpoint triggerDBSyncEndpoint
        = reconTestInjector.getInstance(TriggerDBSyncEndpoint.class);
    Response response = triggerDBSyncEndpoint.triggerOMDBSync();
    assertEquals(200, response.getStatus());
    assertEquals(true, response.getEntity());
  }
}
