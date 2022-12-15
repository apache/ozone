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

package org.apache.hadoop.ozone.recon.spi.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeEmptyOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LOOP_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconUtils.createTarFile;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmDeltaRequest;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmSnapshotRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.nio.file.Paths;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.SequenceNumberNotFoundException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.metrics.OzoneManagerSyncMetrics;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.OMDBUpdatesHandler;
import org.apache.hadoop.ozone.recon.tasks.OMUpdateEventBatch;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;

import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.rocksdb.TransactionLogIterator.BatchResult;
import org.rocksdb.WriteBatch;

/**
 * Class to test Ozone Manager Service Provider Implementation.
 */
public class TestOzoneManagerServiceProviderImpl {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration configuration;
  private OzoneManagerProtocol ozoneManagerProtocol;

  @Before
  public void setUp() throws Exception {
    configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    configuration.set(OZONE_RECON_DB_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    configuration.set("ozone.om.address", "localhost:9862");
    ozoneManagerProtocol = getMockOzoneManagerClient(new DBUpdates());
  }

  @Test
  public void testUpdateReconOmDBWithNewSnapshot() throws Exception {

    OMMetadataManager omMetadataManager =
        initializeNewOmMetadataManager(temporaryFolder.newFolder());
    ReconOMMetadataManager reconOMMetadataManager =
        getTestReconOmMetadataManager(omMetadataManager,
            temporaryFolder.newFolder());

    writeDataToOm(omMetadataManager, "key_one");
    writeDataToOm(omMetadataManager, "key_two");

    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    File tarFile = createTarFile(checkpoint.getCheckpointLocation());
    InputStream inputStream = new FileInputStream(tarFile);
    ReconUtils reconUtilsMock = getMockReconUtils();
    HttpURLConnection httpURLConnectionMock = mock(HttpURLConnection.class);
    when(httpURLConnectionMock.getInputStream()).thenReturn(inputStream);
    when(reconUtilsMock.makeHttpCall(any(), anyString(), anyBoolean()))
        .thenReturn(httpURLConnectionMock);

    ReconTaskController reconTaskController = getMockTaskController();

    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new OzoneManagerServiceProviderImpl(configuration,
            reconOMMetadataManager, reconTaskController, reconUtilsMock,
            ozoneManagerProtocol);

    Assert.assertNull(reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_one"));
    Assert.assertNull(reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_two"));

    assertTrue(ozoneManagerServiceProvider.updateReconOmDBWithNewSnapshot());

    assertNotNull(reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_one"));
    assertNotNull(reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_two"));
  }

  @Test
  public void testGetOzoneManagerDBSnapshot() throws Exception {

    File reconOmSnapshotDbDir = temporaryFolder.newFolder();

    File checkpointDir = Paths.get(reconOmSnapshotDbDir.getAbsolutePath(),
        "testGetOzoneManagerDBSnapshot").toFile();
    checkpointDir.mkdir();

    File file1 = Paths.get(checkpointDir.getAbsolutePath(), "file1")
        .toFile();
    String str = "File1 Contents";
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(file1), UTF_8));
      writer.write(str);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    File file2 = Paths.get(checkpointDir.getAbsolutePath(), "file2")
        .toFile();
    str = "File2 Contents";
    try {
      writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(file2), UTF_8));
      writer.write(str);
    } finally {
      writer.close();
    }

    //Create test tar file.
    File tarFile = createTarFile(checkpointDir.toPath());
    InputStream fileInputStream = new FileInputStream(tarFile);
    ReconUtils reconUtilsMock = getMockReconUtils();
    HttpURLConnection httpURLConnectionMock = mock(HttpURLConnection.class);
    when(httpURLConnectionMock.getInputStream()).thenReturn(fileInputStream);
    when(reconUtilsMock.makeHttpCall(any(), anyString(), anyBoolean()))
        .thenReturn(httpURLConnectionMock);

    ReconOMMetadataManager reconOMMetadataManager =
        mock(ReconOMMetadataManager.class);
    ReconTaskController reconTaskController = getMockTaskController();
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new OzoneManagerServiceProviderImpl(configuration,
            reconOMMetadataManager, reconTaskController, reconUtilsMock,
            ozoneManagerProtocol);

    DBCheckpoint checkpoint = ozoneManagerServiceProvider
        .getOzoneManagerDBSnapshot();
    assertNotNull(checkpoint);
    assertTrue(checkpoint.getCheckpointLocation().toFile().isDirectory());

    File[] files = checkpoint.getCheckpointLocation().toFile().listFiles();
    assertNotNull(files);
    assertEquals(2, files.length);
  }


  static RocksDatabase getRocksDatabase(OMMetadataManager om) {
    return ((RDBStore)om.getStore()).getDb();
  }

  @Test
  public void testGetAndApplyDeltaUpdatesFromOM() throws Exception {

    // Writing 2 Keys into a source OM DB and collecting it in a
    // DBUpdatesWrapper.
    OMMetadataManager sourceOMMetadataMgr =
        initializeNewOmMetadataManager(temporaryFolder.newFolder());
    writeDataToOm(sourceOMMetadataMgr, "key_one");
    writeDataToOm(sourceOMMetadataMgr, "key_two");

    final RocksDatabase rocksDB = getRocksDatabase(sourceOMMetadataMgr);
    ManagedTransactionLogIterator logIterator = rocksDB.getUpdatesSince(0L);
    DBUpdates dbUpdatesWrapper = new DBUpdates();
    while (logIterator.get().isValid()) {
      BatchResult result = logIterator.get().getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      dbUpdatesWrapper.addWriteBatch(writeBatch.data(),
          result.sequenceNumber());
      logIterator.get().next();
    }

    // OM Service Provider's Metadata Manager.
    OMMetadataManager omMetadataManager =
        initializeNewOmMetadataManager(temporaryFolder.newFolder());

    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new OzoneManagerServiceProviderImpl(configuration,
            getTestReconOmMetadataManager(omMetadataManager,
                temporaryFolder.newFolder()),
            getMockTaskController(), new ReconUtils(),
            getMockOzoneManagerClient(dbUpdatesWrapper));

    OMDBUpdatesHandler updatesHandler =
        new OMDBUpdatesHandler(omMetadataManager);
    ozoneManagerServiceProvider.getAndApplyDeltaUpdatesFromOM(
        0L, updatesHandler);

    OzoneManagerSyncMetrics metrics = ozoneManagerServiceProvider.getMetrics();
    assertEquals(4.0,
        metrics.getAverageNumUpdatesInDeltaRequest().value(), 0.0);
    assertEquals(1, metrics.getNumNonZeroDeltaRequests().value());

    // In this method, we have to assert the "GET" path and the "APPLY" path.

    // Assert GET path --> verify if the OMDBUpdatesHandler picked up the 4
    // events ( 1 Vol PUT + 1 Bucket PUT + 2 Key PUTs).
    assertEquals(4, updatesHandler.getEvents().size());

    // Assert APPLY path --> Verify if the OM service provider's RocksDB got
    // the changes.
    String fullKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", "key_one");
    assertTrue(ozoneManagerServiceProvider.getOMMetadataManagerInstance()
        .getKeyTable(getBucketLayout()).isExist(fullKey));
    fullKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", "key_two");
    assertTrue(ozoneManagerServiceProvider.getOMMetadataManagerInstance()
        .getKeyTable(getBucketLayout()).isExist(fullKey));
  }

  @Test
  public void testGetAndApplyDeltaUpdatesFromOMWithLimit() throws Exception {

    // Writing 2 Keys into a source OM DB and collecting it in a
    // DBUpdatesWrapper.
    OMMetadataManager sourceOMMetadataMgr =
        initializeNewOmMetadataManager(temporaryFolder.newFolder());
    writeDataToOm(sourceOMMetadataMgr, "key_one");
    writeDataToOm(sourceOMMetadataMgr, "key_two");

    final RocksDatabase rocksDB = getRocksDatabase(sourceOMMetadataMgr);
    ManagedTransactionLogIterator logIterator = rocksDB.getUpdatesSince(0L);
    DBUpdates[] dbUpdatesWrapper = new DBUpdates[4];
    int index = 0;
    while (logIterator.get().isValid()) {
      BatchResult result = logIterator.get().getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      dbUpdatesWrapper[index] = new DBUpdates();
      dbUpdatesWrapper[index].addWriteBatch(writeBatch.data(),
          result.sequenceNumber());
      index++;
      logIterator.get().next();
    }

    // OM Service Provider's Metadata Manager.
    OMMetadataManager omMetadataManager =
        initializeNewOmMetadataManager(temporaryFolder.newFolder());

    OzoneConfiguration withLimitConfiguration =
        new OzoneConfiguration(configuration);
    withLimitConfiguration.setLong(RECON_OM_DELTA_UPDATE_LIMIT, 1);
    withLimitConfiguration.setLong(RECON_OM_DELTA_UPDATE_LOOP_LIMIT, 3);
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new OzoneManagerServiceProviderImpl(withLimitConfiguration,
            getTestReconOmMetadataManager(omMetadataManager,
                temporaryFolder.newFolder()),
            getMockTaskController(), new ReconUtils(),
            getMockOzoneManagerClientWith4Updates(dbUpdatesWrapper[0],
                dbUpdatesWrapper[1], dbUpdatesWrapper[2], dbUpdatesWrapper[3]));

    OMDBUpdatesHandler updatesHandler =
        new OMDBUpdatesHandler(omMetadataManager);
    ozoneManagerServiceProvider.getAndApplyDeltaUpdatesFromOM(
        0L, updatesHandler);

    OzoneManagerSyncMetrics metrics = ozoneManagerServiceProvider.getMetrics();
    assertEquals(1.0,
        metrics.getAverageNumUpdatesInDeltaRequest().value(), 0.0);
    assertEquals(3, metrics.getNumNonZeroDeltaRequests().value());

    // In this method, we have to assert the "GET" path and the "APPLY" path.

    // Assert GET path --> verify if the OMDBUpdatesHandler picked up the first
    // 3 of 4 events ( 1 Vol PUT + 1 Bucket PUT + 2 Key PUTs).
    assertEquals(3, updatesHandler.getEvents().size());

    // Assert APPLY path --> Verify if the OM service provider's RocksDB got
    // the first 3 changes, last change not applied.
    String fullKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", "key_one");
    assertTrue(ozoneManagerServiceProvider.getOMMetadataManagerInstance()
        .getKeyTable(getBucketLayout()).isExist(fullKey));
    fullKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", "key_two");
    assertFalse(ozoneManagerServiceProvider.getOMMetadataManagerInstance()
        .getKeyTable(getBucketLayout()).isExist(fullKey));
  }

  @Test
  public void testSyncDataFromOMFullSnapshot() throws Exception {

    // Empty OM DB to start with.
    ReconOMMetadataManager omMetadataManager = getTestReconOmMetadataManager(
        initializeEmptyOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    ReconTaskStatusDao reconTaskStatusDaoMock =
        mock(ReconTaskStatusDao.class);
    doNothing().when(reconTaskStatusDaoMock)
        .update(any(ReconTaskStatus.class));

    ReconTaskController reconTaskControllerMock = getMockTaskController();
    when(reconTaskControllerMock.getReconTaskStatusDao())
        .thenReturn(reconTaskStatusDaoMock);
    doNothing().when(reconTaskControllerMock)
        .reInitializeTasks(omMetadataManager);

    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new MockOzoneServiceProvider(configuration, omMetadataManager,
            reconTaskControllerMock, new ReconUtils(), ozoneManagerProtocol);

    OzoneManagerSyncMetrics metrics = ozoneManagerServiceProvider.getMetrics();
    assertEquals(0, metrics.getNumSnapshotRequests().value());

    // Should trigger full snapshot request.
    ozoneManagerServiceProvider.syncDataFromOM();

    ArgumentCaptor<ReconTaskStatus> captor =
        ArgumentCaptor.forClass(ReconTaskStatus.class);
    verify(reconTaskStatusDaoMock, times(1))
        .update(captor.capture());
    assertEquals(OmSnapshotRequest.name(), captor.getValue().getTaskName());
    verify(reconTaskControllerMock, times(1))
        .reInitializeTasks(omMetadataManager);
    assertEquals(1, metrics.getNumSnapshotRequests().value());
  }

  @Test
  public void testSyncDataFromOMDeltaUpdates() throws Exception {

    // Non-Empty OM DB to start with.
    ReconOMMetadataManager omMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    ReconTaskStatusDao reconTaskStatusDaoMock =
        mock(ReconTaskStatusDao.class);
    doNothing().when(reconTaskStatusDaoMock)
        .update(any(ReconTaskStatus.class));

    ReconTaskController reconTaskControllerMock = getMockTaskController();
    when(reconTaskControllerMock.getReconTaskStatusDao())
        .thenReturn(reconTaskStatusDaoMock);
    doNothing().when(reconTaskControllerMock)
        .consumeOMEvents(any(OMUpdateEventBatch.class),
            any(OMMetadataManager.class));

    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new OzoneManagerServiceProviderImpl(configuration, omMetadataManager,
            reconTaskControllerMock, new ReconUtils(), ozoneManagerProtocol);

    OzoneManagerSyncMetrics metrics = ozoneManagerServiceProvider.getMetrics();

    // Should trigger delta updates.
    ozoneManagerServiceProvider.syncDataFromOM();

    ArgumentCaptor<ReconTaskStatus> captor =
        ArgumentCaptor.forClass(ReconTaskStatus.class);
    verify(reconTaskStatusDaoMock, times(1))
        .update(captor.capture());
    assertEquals(OmDeltaRequest.name(), captor.getValue().getTaskName());

    verify(reconTaskControllerMock, times(1))
        .consumeOMEvents(any(OMUpdateEventBatch.class),
            any(OMMetadataManager.class));
    assertEquals(0, metrics.getNumSnapshotRequests().value());
  }

  @Test
  public void testSyncDataFromOMFullSnapshotForSNNFE() throws Exception {

    // Non-Empty OM DB to start with.
    ReconOMMetadataManager omMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());
    ReconTaskStatusDao reconTaskStatusDaoMock =
        mock(ReconTaskStatusDao.class);
    doNothing().when(reconTaskStatusDaoMock)
        .update(any(ReconTaskStatus.class));

    ReconTaskController reconTaskControllerMock = getMockTaskController();
    when(reconTaskControllerMock.getReconTaskStatusDao())
        .thenReturn(reconTaskStatusDaoMock);
    doNothing().when(reconTaskControllerMock)
        .reInitializeTasks(omMetadataManager);

    OzoneManagerProtocol protocol = getMockOzoneManagerClientWithThrow();
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        new MockOzoneServiceProvider(configuration, omMetadataManager,
            reconTaskControllerMock, new ReconUtils(), protocol);

    OzoneManagerSyncMetrics metrics = ozoneManagerServiceProvider.getMetrics();

    // Should trigger full snapshot request.
    ozoneManagerServiceProvider.syncDataFromOM();

    ArgumentCaptor<ReconTaskStatus> captor =
        ArgumentCaptor.forClass(ReconTaskStatus.class);
    verify(reconTaskStatusDaoMock, times(1))
        .update(captor.capture());
    assertEquals(OmSnapshotRequest.name(), captor.getValue().getTaskName());
    verify(reconTaskControllerMock, times(1))
        .reInitializeTasks(omMetadataManager);
    assertEquals(1, metrics.getNumSnapshotRequests().value());
  }

  private ReconTaskController getMockTaskController() {
    return mock(ReconTaskController.class);
  }

  private ReconUtils getMockReconUtils() throws IOException {
    ReconUtils reconUtilsMock = mock(ReconUtils.class);
    when(reconUtilsMock.getReconDbDir(any(), anyString())).thenCallRealMethod();
    doCallRealMethod().when(reconUtilsMock).untarCheckpointFile(any(), any());
    return reconUtilsMock;
  }

  private OzoneManagerProtocol getMockOzoneManagerClient(
      DBUpdates dbUpdatesWrapper) throws IOException {
    OzoneManagerProtocol ozoneManagerProtocolMock =
        mock(OzoneManagerProtocol.class);
    when(ozoneManagerProtocolMock.getDBUpdates(any(OzoneManagerProtocolProtos
        .DBUpdatesRequest.class))).thenReturn(dbUpdatesWrapper);
    return ozoneManagerProtocolMock;
  }

  // Mock the case of SNNFE
  private OzoneManagerProtocol getMockOzoneManagerClientWithThrow()
      throws IOException {
    OzoneManagerProtocol ozoneManagerProtocolMock =
        mock(OzoneManagerProtocol.class);
    when(ozoneManagerProtocolMock.getDBUpdates(any(OzoneManagerProtocolProtos
        .DBUpdatesRequest.class)))
        .thenThrow(new SequenceNumberNotFoundException());
    return ozoneManagerProtocolMock;
  }

  private OzoneManagerProtocol getMockOzoneManagerClientWith4Updates(
      DBUpdates updates1, DBUpdates updates2, DBUpdates updates3,
      DBUpdates updates4) throws IOException {
    OzoneManagerProtocol ozoneManagerProtocolMock =
        mock(OzoneManagerProtocol.class);
    when(ozoneManagerProtocolMock.getDBUpdates(any(OzoneManagerProtocolProtos
        .DBUpdatesRequest.class))).thenReturn(updates1, updates2, updates3,
        updates4);
    return ozoneManagerProtocolMock;
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}

/**
 * Mock OzoneManagerServiceProviderImpl which overrides
 * updateReconOmDBWithNewSnapshot.
 */
class MockOzoneServiceProvider extends OzoneManagerServiceProviderImpl {

  MockOzoneServiceProvider(OzoneConfiguration configuration,
                           ReconOMMetadataManager omMetadataManager,
                           ReconTaskController reconTaskController,
                           ReconUtils reconUtils,
                           OzoneManagerProtocol ozoneManagerClient) {
    super(configuration, omMetadataManager, reconTaskController, reconUtils,
        ozoneManagerClient);
  }

  @Override
  public boolean updateReconOmDBWithNewSnapshot() {
    return true;
  }
}
