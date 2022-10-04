package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconUtils.createTarFile;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class TestOMSyncEndpoint {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OMSyncEndpoint omSyncEndpoint;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;
  private OzoneConfiguration configuration;
  private OzoneManagerProtocol ozoneManagerProtocol;
  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;

  @Before
  public void setUp() throws IOException, AuthenticationException {
    configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    configuration.set(OZONE_RECON_DB_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    configuration.set("ozone.om.address", "localhost:9862");

    ozoneManagerProtocol = mock(OzoneManagerProtocol.class);
    when(ozoneManagerProtocol.getDBUpdates(any(OzoneManagerProtocolProtos
        .DBUpdatesRequest.class))).thenReturn(new DBUpdates());

    omMetadataManager =
        initializeNewOmMetadataManager(temporaryFolder.newFolder());
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
            temporaryFolder.newFolder());

    ReconUtils reconUtilsMock = mock(ReconUtils.class);
    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    File tarFile = createTarFile(checkpoint.getCheckpointLocation());
    InputStream inputStream = new FileInputStream(tarFile);
    HttpURLConnection httpURLConnectionMock = mock(HttpURLConnection.class);
    when(httpURLConnectionMock.getInputStream()).thenReturn(inputStream);
    when(reconUtilsMock.makeHttpCall(any(), anyString(), anyBoolean()))
        .thenReturn(httpURLConnectionMock);

    ReconTaskController reconTaskController = mock(ReconTaskController.class);
    when(reconTaskController.getReconTaskStatusDao())
        .thenReturn(mock(ReconTaskStatusDao.class));

    ozoneManagerServiceProvider =
        new OzoneManagerServiceProviderImpl(configuration,
            reconOMMetadataManager, reconTaskController, reconUtilsMock,
            ozoneManagerProtocol);
    ozoneManagerServiceProvider.start();

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
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

    omSyncEndpoint = reconTestInjector.getInstance(OMSyncEndpoint.class);
  }

  @Test
  public void testOMSyncEndpoint() {
    Response response = omSyncEndpoint.triggerOMSync();
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(true, response.getEntity());
  }
}
