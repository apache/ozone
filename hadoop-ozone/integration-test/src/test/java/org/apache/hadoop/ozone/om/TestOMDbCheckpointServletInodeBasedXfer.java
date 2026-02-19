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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.InodeMetadataRocksDBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.DAGLeveledResource;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for testing the OM DB Checkpoint provider servlet using inode based transfer logic.
 */
public class TestOMDbCheckpointServletInodeBasedXfer {

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneManager om;
  private OzoneConfiguration conf;
  @TempDir
  private Path folder;
  private HttpServletRequest requestMock = null;
  private HttpServletResponse responseMock = null;
  private OMDBCheckpointServletInodeBasedXfer omDbCheckpointServletMock = null;
  private ServletOutputStream servletOutputStream;
  private File tempFile;
  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOMDbCheckpointServletInodeBasedXfer.class);

  @BeforeEach
  void init() throws Exception {
    conf = new OzoneConfiguration();
    // ensure cache entries are not evicted thereby snapshot db's are not closed
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL,
        100, TimeUnit.MINUTES);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
  }

  @AfterEach
  void shutdown() {
    IOUtils.closeQuietly(client, cluster);
    cluster = null;
  }

  private void setupCluster() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    OzoneManager normalOm = cluster.getOzoneManager();
    om = spy(normalOm);
  }

  private void setupMocks() throws Exception {
    final Path tempPath = folder.resolve("temp" + COUNTER.incrementAndGet() + ".tar");
    tempFile = tempPath.toFile();

    servletOutputStream = new ServletOutputStream() {
      private final OutputStream fileOutputStream = Files.newOutputStream(tempPath);

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void setWriteListener(WriteListener writeListener) {
      }

      @Override
      public void close() throws IOException {
        fileOutputStream.close();
        super.close();
      }

      @Override
      public void write(int b) throws IOException {
        fileOutputStream.write(b);
      }
    };

    omDbCheckpointServletMock = mock(OMDBCheckpointServletInodeBasedXfer.class);

    BootstrapStateHandler.Lock lock = null;
    if (om != null) {
      lock = new OMDBCheckpointServlet.Lock(om);
    }
    doCallRealMethod().when(omDbCheckpointServletMock).init();
    assertNull(doCallRealMethod().when(omDbCheckpointServletMock).getDbStore());

    requestMock = mock(HttpServletRequest.class);
    // Return current user short name when asked
    when(requestMock.getRemoteUser())
        .thenReturn(UserGroupInformation.getCurrentUser().getShortUserName());
    responseMock = mock(HttpServletResponse.class);

    ServletContext servletContextMock = mock(ServletContext.class);
    when(omDbCheckpointServletMock.getServletContext())
        .thenReturn(servletContextMock);

    when(servletContextMock.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .thenReturn(om);
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH))
        .thenReturn("true");

    doCallRealMethod().when(omDbCheckpointServletMock).doGet(requestMock,
        responseMock);
    doCallRealMethod().when(omDbCheckpointServletMock).doPost(requestMock,
        responseMock);

    doCallRealMethod().when(omDbCheckpointServletMock)
        .writeDbDataToStream(any(), any(), any(), any(), any());
    doCallRealMethod().when(omDbCheckpointServletMock)
        .collectFilesFromDir(any(), any(), any(), anyBoolean(), any());
    doCallRealMethod().when(omDbCheckpointServletMock).collectDbDataToTransfer(any(), any(), any());

    when(omDbCheckpointServletMock.getBootstrapStateLock())
        .thenReturn(lock);
    doCallRealMethod().when(omDbCheckpointServletMock).getCheckpoint(any(), anyBoolean());
    assertNull(doCallRealMethod().when(omDbCheckpointServletMock).getBootstrapTempData());
    doCallRealMethod().when(omDbCheckpointServletMock).
        processMetadataSnapshotRequest(any(), any(), anyBoolean(), anyBoolean());
    doCallRealMethod().when(omDbCheckpointServletMock).getCompactionLogDir();
    doCallRealMethod().when(omDbCheckpointServletMock).getSstBackupDir();
    doCallRealMethod().when(omDbCheckpointServletMock)
        .collectSnapshotData(anySet(),  anyCollection(), anyCollection(), any(), any());
    doCallRealMethod().when(omDbCheckpointServletMock).createAndPrepareCheckpoint(anyBoolean());
    doCallRealMethod().when(omDbCheckpointServletMock).getSnapshotDirsFromDB(any(), any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTarballBatching(boolean includeSnapshots) throws Exception {
    String volumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck" + RandomStringUtils.secure().nextNumeric(5);
    AtomicReference<DBCheckpoint> realCheckpoint = new AtomicReference<>();
    setupClusterAndMocks(volumeName, bucketName, realCheckpoint, includeSnapshots);
    long maxFileSizeLimit = 4096;
    om.getConfiguration().setLong(OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY, maxFileSizeLimit);
    // Get the tarball.
    omDbCheckpointServletMock.doGet(requestMock, responseMock);
    String testDirName = folder.resolve("testDir").toString();
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    File newDbDir = new File(newDbDirName);
    assertTrue(newDbDir.mkdirs());
    FileUtil.unTar(tempFile, newDbDir);
    long totalSize;
    try (Stream<Path> list = Files.list(newDbDir.toPath())) {
      totalSize = list.mapToLong(path -> path.toFile().length()).sum();
    }
    boolean obtainedFilesUnderMaxLimit = totalSize < maxFileSizeLimit;
    if (!includeSnapshots) {
      // If includeSnapshotData flag is set to false , it always sends all data
      // in one batch and doesn't respect the max size config. This is how Recon
      // uses it today.
      assertFalse(obtainedFilesUnderMaxLimit);
    } else {
      assertTrue(obtainedFilesUnderMaxLimit);
    }
  }

  @Test
  public void testWriteDBToArchiveClosesFilesListStream() throws Exception {
    OMDBCheckpointServletInodeBasedXfer servlet = new OMDBCheckpointServletInodeBasedXfer();

    final Path dbDir = Files.createTempDirectory(folder, "dbdir-");
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Stream<Path> stream = Stream.<Path>empty().onClose(() -> closed.set(true));

    // Do not use CALLS_REAL_METHODS for java.nio.file.Files: internal/private static
    // methods (eg Files.provider()) get intercepted too and Mockito will try to invoke
    // them reflectively, which fails on JDK9+ without --add-opens.
    try (MockedStatic<Files> files = mockStatic(Files.class)) {
      files.when(() -> Files.exists(dbDir)).thenReturn(true);
      files.when(() -> Files.list(dbDir)).thenReturn(stream);

      OMDBArchiver archiver = new OMDBArchiver();
      archiver.setTmpDir(folder);
      boolean result = servlet.collectFilesFromDir(new HashSet<>(), dbDir,
          new AtomicLong(Long.MAX_VALUE), true, archiver);
      assertTrue(result);
    }

    assertTrue(closed.get(), "Files.list() stream should be closed to avoid FD leaks");
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testContentsOfTarballWithSnapshot(boolean includeSnapshot) throws Exception {
    String volumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck" + RandomStringUtils.secure().nextNumeric(5);
    AtomicReference<DBCheckpoint> realCheckpoint = new AtomicReference<>();
    setupClusterAndMocks(volumeName, bucketName, realCheckpoint, includeSnapshot);
    DBStore dbStore = om.getMetadataManager().getStore();
    // Get the tarball.
    omDbCheckpointServletMock.doGet(requestMock, responseMock);
    String testDirName = folder.resolve("testDir").toString();
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    File newDbDir = new File(newDbDirName);
    assertTrue(newDbDir.mkdirs());
    FileUtil.unTar(tempFile, newDbDir);
    List<String> snapshotPaths = new ArrayList<>();
    client.getObjectStore().listSnapshot(volumeName, bucketName, "", null)
        .forEachRemaining(snapInfo -> snapshotPaths.add(getSnapshotDBPath(snapInfo.getCheckpointDir())));
    Set<String> inodesFromOmDataDir = new HashSet<>();
    Set<String> inodesFromTarball = new HashSet<>();
    try (Stream<Path> filesInTarball = Files.list(newDbDir.toPath())) {
      List<Path> files = filesInTarball.collect(Collectors.toList());
      for (Path p : files) {
        File file = p.toFile();
        if (file.getName().equals(OmSnapshotManager.OM_HARDLINK_FILE)) {
          continue;
        }
        String inode = getInode(file.getName());
        inodesFromTarball.add(inode);
      }
    }
    Map<String, List<String>> hardLinkMapFromOmData = new HashMap<>();
    Path checkpointLocation = realCheckpoint.get().getCheckpointLocation();
    populateInodesOfFilesInDirectory(dbStore, checkpointLocation,
        inodesFromOmDataDir, hardLinkMapFromOmData);
    int numSnapshots = 0;
    if (includeSnapshot) {
      for (String snapshotPath : snapshotPaths) {
        populateInodesOfFilesInDirectory(dbStore, Paths.get(snapshotPath),
            inodesFromOmDataDir, hardLinkMapFromOmData);
        numSnapshots++;
      }
    }
    populateInodesOfFilesInDirectory(dbStore, Paths.get(dbStore.getRocksDBCheckpointDiffer().getSSTBackupDir()),
        inodesFromOmDataDir, hardLinkMapFromOmData);
    Path hardlinkFilePath =
        newDbDir.toPath().resolve(OmSnapshotManager.OM_HARDLINK_FILE);
    Map<String, List<String>> hardlinkMapFromTarball = readFileToMap(hardlinkFilePath.toString());

    // verify that all entries in hardLinkMapFromOmData are present in hardlinkMapFromTarball.
    // entries in hardLinkMapFromOmData are from the snapshots + OM db checkpoint so they
    // should be present in the tarball.

    for (Map.Entry<String, List<String>> entry : hardLinkMapFromOmData.entrySet()) {
      String key = entry.getKey();
      List<String> value = entry.getValue();
      assertTrue(hardlinkMapFromTarball.containsKey(key));
      assertEquals(value, hardlinkMapFromTarball.get(key));
    }
    // all files from the checkpoint should be in the tarball
    assertFalse(inodesFromTarball.isEmpty());
    assertTrue(inodesFromTarball.containsAll(inodesFromOmDataDir));

    long actualYamlFiles = Files.list(newDbDir.toPath())
        .filter(f -> f.getFileName().toString()
            .endsWith(".yaml")).count();
    assertEquals(numSnapshots, actualYamlFiles,
        "Number of generated YAML files should match the number of snapshots.");

    InodeMetadataRocksDBCheckpoint obtainedCheckpoint =
        new InodeMetadataRocksDBCheckpoint(newDbDir.toPath());
    assertNotNull(obtainedCheckpoint);

    if (includeSnapshot) {
      List<String> yamlRelativePaths = snapshotPaths.stream().map(path -> {
        int startIndex = path.indexOf("db.snapshots");
        if (startIndex != -1) {
          return path.substring(startIndex) + ".yaml";
        }
        return path + ".yaml";
      }).collect(Collectors.toList());

      for (String yamlRelativePath : yamlRelativePaths) {
        String yamlFileName = Paths.get(newDbDir.getPath(),
            yamlRelativePath).toString();
        assertTrue(Files.exists(Paths.get(yamlFileName)));
      }
    }

    assertFalse(hardlinkFilePath.toFile().exists());
  }

  /**
   * Verifies that a manually added entry to the snapshot's delete table
   * is persisted and can be retrieved from snapshot db loaded from OM DB checkpoint.
   */
  @Test
  public void testSnapshotDBConsistency() throws Exception {
    String volumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck" + RandomStringUtils.secure().nextNumeric(5);
    AtomicReference<DBCheckpoint> realCheckpoint = new AtomicReference<>();
    setupClusterAndMocks(volumeName, bucketName, realCheckpoint, true);
    List<OzoneSnapshot> snapshots = new ArrayList<>();
    client.getObjectStore().listSnapshot(volumeName, bucketName, "", null)
        .forEachRemaining(snapshots::add);
    OzoneSnapshot snapshotToModify = snapshots.get(0);
    String dummyKey = "dummyKey";
    writeDummyKeyToDeleteTableOfSnapshotDB(snapshotToModify, bucketName, volumeName, dummyKey);
    // Get the tarball.
    omDbCheckpointServletMock.doGet(requestMock, responseMock);
    String testDirName = folder.resolve("testDir").toString();
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    File newDbDir = new File(newDbDirName);
    assertTrue(newDbDir.mkdirs());
    FileUtil.unTar(tempFile, newDbDir);
    InodeMetadataRocksDBCheckpoint obtainedCheckpoint =
        new InodeMetadataRocksDBCheckpoint(newDbDir.toPath());
    assertNotNull(obtainedCheckpoint);
    Path snapshotDbDir = Paths.get(newDbDir.getPath(),
        OM_SNAPSHOT_CHECKPOINT_DIR, OM_DB_NAME + "-" + snapshotToModify.getSnapshotId());
    assertTrue(Files.exists(snapshotDbDir));
    String value = getValueFromSnapshotDeleteTable(dummyKey, snapshotDbDir.toString());
    assertNotNull(value);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteDBToArchive(boolean expectOnlySstFiles) throws Exception {
    setupMocks();
    Path dbDir = folder.resolve("db_data");
    Files.createDirectories(dbDir);
    // Create dummy files: one SST, one non-SST
    Path sstFile = dbDir.resolve("test.sst");
    Files.write(sstFile, "sst content".getBytes(StandardCharsets.UTF_8)); // Write some content to make it non-empty
    Path nonSstFile = dbDir.resolve("test.log");
    Files.write(nonSstFile, "log content".getBytes(StandardCharsets.UTF_8));
    Set<String> sstFilesToExclude = new HashSet<>();
    AtomicLong maxTotalSstSize = new AtomicLong(1000000); // Sufficient size
    OMDBArchiver omDbArchiver = new OMDBArchiver();
    Path tmpDir = folder.resolve("tmp");
    Files.createDirectories(tmpDir);
    omDbArchiver.setTmpDir(tmpDir);
    OMDBArchiver omDbArchiverSpy = spy(omDbArchiver);
    List<String> fileNames = new ArrayList<>();
    doAnswer((invocation) -> {
      File sourceFile = invocation.getArgument(0);
      fileNames.add(sourceFile.getName());
      omDbArchiver.recordFileEntry(sourceFile, invocation.getArgument(1));
      return null;
    }).when(omDbArchiverSpy).recordFileEntry(any(), anyString());
    boolean success =
        omDbCheckpointServletMock.collectFilesFromDir(sstFilesToExclude, dbDir, maxTotalSstSize, expectOnlySstFiles,
            omDbArchiverSpy);
    assertTrue(success);
    verify(omDbArchiverSpy, times(fileNames.size())).recordFileEntry(any(), anyString());
    boolean containsNonSstFile = false;
    for (String fileName : fileNames) {
      if (expectOnlySstFiles) {
        assertTrue(fileName.endsWith(".sst"), "File is not an SST File");
      } else {
        containsNonSstFile = true;
      }
    }

    if (!expectOnlySstFiles) {
      assertTrue(containsNonSstFile, "SST File is not expected");
    }
  }

  /**
   * Verifies that snapshot cache lock coordinates between checkpoint and purge operations,
   * preventing race conditions on follower OM where snapshot directory could be deleted
   * while checkpoint is reading snapshot data.
   *
   * Test steps:
   * 1. Create keys
   * 2. Create snapshot 1
   * 3. Create snapshot 2
   * 4. Delete snapshot 2 (marks it as DELETED)
   * 5. Stop SnapshotDeletingService to prevent automatic purge
   * 6. Invoke checkpoint servlet (acquires bootstrap lock and snapshot cache lock)
   * 7. Submit purge request for snapshot 2 during checkpoint processing (simulates Ratis transaction on follower)
   * 8. Verify purge waits for snapshot cache lock (blocked while checkpoint holds it)
   * 9. Verify checkpoint completes first and tarball includes snapshot 2 data
   * 10. Verify purge completes after checkpoint releases snapshot cache lock
   *
   * @throws Exception if test setup or execution fails
   */
  @Test
  public void testBootstrapOnFollowerConsistency() throws Exception {
    String volumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck" + RandomStringUtils.secure().nextNumeric(5);
    setupCluster();
    om.getKeyManager().getSnapshotSstFilteringService().pause();
    om.getKeyManager().getSnapshotDeletingService().suspend();
    // Create test data and snapshots
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName);
    // Create key before first snapshot
    TestDataUtil.createKey(bucket, "key1",
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
        "data1".getBytes(StandardCharsets.UTF_8));
    client.getObjectStore().createSnapshot(volumeName, bucketName, "snapshot1");
    client.getObjectStore().createSnapshot(volumeName, bucketName, "snapshot2");
    List<OzoneSnapshot> snapshots = new ArrayList<>();
    client.getObjectStore().listSnapshot(volumeName, bucketName, "", null)
        .forEachRemaining(snapshots::add);
    assertEquals(2, snapshots.size(), "Should have 2 snapshots initially");
    OzoneSnapshot snapshot1 = snapshots.stream()
        .filter(snap -> snap.getName().equals("snapshot1"))
        .findFirst().get();
    OzoneSnapshot snapshot2 = snapshots.stream()
        .filter(snap -> snap.getName().equals("snapshot2")).findFirst().get();
    assertEquals(2, snapshots.size(), "Should have 2 snapshots initially");
    waitTillSnapshotInDeletedState(volumeName, bucketName, snapshot2);
    // Setup servlet mocks for checkpoint processing
    setupMocks();
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA)).thenReturn("true");
    CountDownLatch purgeSubmitted = new CountDownLatch(1);
    AtomicLong checkpointEndTime = new AtomicLong(0);
    AtomicLong purgeEndTime = new AtomicLong(0);

    DBStore dbStore = om.getMetadataManager().getStore();
    DBStore spyDbStore = spy(dbStore);
    AtomicReference<DBCheckpoint> capturedCheckpoint = new AtomicReference<>();
    when(spyDbStore.getCheckpoint(true)).thenAnswer(invocation -> {
      // Submit purge request in background thread (simulating Ratis transaction on follower)
      Thread purgeThread = new Thread(() -> {
        try {
          String snapshotTableKey = SnapshotInfo.getTableKey(volumeName, bucketName, snapshot2.getName());
          // Construct SnapshotPurge request
          OzoneManagerProtocolProtos.SnapshotPurgeRequest snapshotPurgeRequest =
              OzoneManagerProtocolProtos.SnapshotPurgeRequest.newBuilder()
                  .addSnapshotDBKeys(snapshotTableKey)
                  .build();

          OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
              .setCmdType(OzoneManagerProtocolProtos.Type.SnapshotPurge)
              .setSnapshotPurgeRequest(snapshotPurgeRequest)
              .setClientId(UUID.randomUUID().toString())
              .build();

          purgeSubmitted.countDown();
          long purgeStartTime = System.currentTimeMillis();
          // Submit via Ratis (simulating follower receiving transaction)
          // This will trigger OMSnapshotPurgeResponse which needs SNAPSHOT_DB_LOCK
          ClientId clientId = ClientId.randomId();
          long callId = 1;
          OzoneManagerProtocolProtos.OMResponse
              response = om.getOmRatisServer().submitRequest(omRequest, clientId, callId);

          if (response.getSuccess()) {
            // Wait for purge to complete (snapshot removed from table)
            GenericTestUtils.waitFor(() -> {
              try {
                boolean purged = om.getMetadataManager().getSnapshotInfoTable().get(snapshotTableKey) == null;
                if (purged) {
                  purgeEndTime.set(System.currentTimeMillis());
                  long duration = purgeEndTime.get() - purgeStartTime;
                  LOG.info("Purge completed in {} ms", duration);
                }
                return purged;
              } catch (Exception ex) {
                return false;
              }
            }, 100, 40_000);
          }
        } catch (Exception e) {
          LOG.error("Purge submission failed", e);
        }
      });
      purgeThread.start();

      // Wait for purge request to be submitted
      assertTrue(purgeSubmitted.await(2, TimeUnit.SECONDS), "Purge should be submitted");
      // Small delay to ensure purge request reaches state machine
      Thread.sleep(200);
      DBCheckpoint checkpoint = spy(dbStore.getCheckpoint(true));
      doNothing().when(checkpoint).cleanupCheckpoint(); // Don't cleanup for verification
      capturedCheckpoint.set(checkpoint);
      return checkpoint;
    });
    // Initialize servlet
    doCallRealMethod().when(omDbCheckpointServletMock).initialize(any(), any(),
        eq(false), any(), any(), eq(false));
    omDbCheckpointServletMock.initialize(spyDbStore, om.getMetrics().getDBCheckpointMetrics(),
        false, om.getOmAdminUsernames(), om.getOmAdminGroups(), false);
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);
    // Process checkpoint servlet
    omDbCheckpointServletMock.doGet(requestMock, responseMock);
    String testDirName = folder.resolve("testDir").toString();
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    File newDbDir = new File(newDbDirName);
    assertTrue(newDbDir.mkdirs());
    FileUtil.unTar(tempFile, newDbDir);
    InodeMetadataRocksDBCheckpoint obtainedCheckpoint =
        new InodeMetadataRocksDBCheckpoint(newDbDir.toPath());
    assertNotNull(obtainedCheckpoint);
    Path snapshot1DbDir = Paths.get(newDbDir.getPath(),  OM_SNAPSHOT_CHECKPOINT_DIR,
        OM_DB_NAME + "-" + snapshot1.getSnapshotId());
    Path snapshot2DbDir = Paths.get(newDbDir.getPath(),  OM_SNAPSHOT_CHECKPOINT_DIR,
        OM_DB_NAME + "-" + snapshot2.getSnapshotId());
    assertTrue(purgeEndTime.get() >= checkpointEndTime.get(),
        "Purge should complete after checkpoint releases snapshot cache lock");

    // Verify snapshot is purged
    List<OzoneSnapshot> snapshotsAfter = new ArrayList<>();
    client.getObjectStore().listSnapshot(volumeName, bucketName, "", null)
        .forEachRemaining(snapshotsAfter::add);
    assertEquals(1, snapshotsAfter.size(), "Snapshot2 should be purged");
    boolean snapshot1IncludedInCheckpoint = Files.exists(snapshot1DbDir);
    boolean snapshot2IncludedInCheckpoint = Files.exists(snapshot2DbDir);
    assertTrue(snapshot1IncludedInCheckpoint && snapshot2IncludedInCheckpoint,
        "Checkpoint should include both snapshot1 and snapshot2 data");
    // Cleanup
    if (capturedCheckpoint.get() != null) {
      capturedCheckpoint.get().cleanupCheckpoint();
    }
  }

  private void waitTillSnapshotInDeletedState(String volumeName, String bucketName, OzoneSnapshot snapshot)
      throws IOException, InterruptedException, TimeoutException {
    String snapshotTableKey = SnapshotInfo.getTableKey(volumeName, bucketName, snapshot.getName());
    // delete snapshot and wait for snapshot to be purged
    client.getObjectStore().deleteSnapshot(volumeName, bucketName, snapshot.getName());
    GenericTestUtils.waitFor(() -> {
      try {
        SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable().get(snapshotTableKey);
        return snapshotInfo != null &&
            snapshotInfo.getSnapshotStatus().name().equals(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED.name());
      } catch (Exception ex) {
        LOG.error("Exception while querying snapshot info for key in cache {}", snapshotTableKey, ex);
        return false;
      }
    }, 100, 30_000);
    om.awaitDoubleBufferFlush();
  }

  @Test
  public void testBootstrapLockCoordination() throws Exception {
    // Mock OMMetadataManager and Store
    OMMetadataManager mockMetadataManager = mock(OMMetadataManager.class);
    DBStore mockStore = mock(DBStore.class);
    when(mockMetadataManager.getStore()).thenReturn(mockStore);
    // Mock OzoneManager
    OzoneManager mockOM = mock(OzoneManager.class);
    when(mockOM.getMetadataManager()).thenReturn(mockMetadataManager);

    IOzoneManagerLock mockOmLock = mock(IOzoneManagerLock.class);
    when(mockOmLock.acquireResourceLock(any())).thenCallRealMethod();
    when(mockOmLock.acquireResourceWriteLock(eq(DAGLeveledResource.BOOTSTRAP_LOCK)))
        .thenReturn(OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED);
    when(mockMetadataManager.getLock()).thenReturn(mockOmLock);
    // Create the actual Lock instance (this tests the real implementation)
    OMDBCheckpointServlet.Lock bootstrapLock = new OMDBCheckpointServlet.Lock(mockOM);
    // Test successful lock acquisition
    UncheckedAutoCloseable result = bootstrapLock.acquireWriteLock();
    // Verify all service locks were acquired
    verify(mockOmLock).acquireResourceWriteLock(eq(DAGLeveledResource.BOOTSTRAP_LOCK));
    // Verify double buffer flush was called
    verify(mockOM).awaitDoubleBufferFlush();
    // Test unlock
    result.close();
    verify(mockOmLock).releaseResourceWriteLock(eq(DAGLeveledResource.BOOTSTRAP_LOCK));

  }

  /**
   * Verifies that bootstrap lock acquisition blocks background services during checkpoint creation,
   * preventing race conditions between checkpoint and service operations.
   */
  @Test
  public void testBootstrapLockBlocksMultipleServices() throws Exception {
    setupCluster();
    // Initialize servlet
    OMDBCheckpointServletInodeBasedXfer servlet = new OMDBCheckpointServletInodeBasedXfer();
    ServletConfig servletConfig = mock(ServletConfig.class);
    ServletContext servletContext = mock(ServletContext.class);
    when(servletConfig.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE)).thenReturn(om);
    servlet.init(servletConfig);

    BootstrapStateHandler.Lock bootstrapLock = servlet.getBootstrapStateLock();
    // Test multiple services being blocked
    CountDownLatch bootstrapAcquired = new CountDownLatch(1);
    CountDownLatch allServicesCompleted = new CountDownLatch(3); // 3 background services
    AtomicInteger servicesBlocked = new AtomicInteger(0);
    AtomicInteger servicesSucceeded = new AtomicInteger(0);
    // Checkpoint thread holds bootstrap lock
    Thread checkpointThread = new Thread(() -> {
      LOG.info("Acquiring bootstrap lock for checkpoint...");
      try (UncheckedAutoCloseable acquired = bootstrapLock.acquireWriteLock()) {
        bootstrapAcquired.countDown();
        Thread.sleep(3000); // Hold for 3 seconds
        LOG.info("Releasing bootstrap lock...");
      } catch (Exception e) {
        fail("Checkpoint failed: " + e.getMessage());
      }
    });

    BiFunction<String, BootstrapStateHandler, Thread> createServiceThread =
        (serviceName, service) -> new Thread(() -> {
          try {
            bootstrapAcquired.await();
            if (service != null) {
              LOG.info("{} : Trying to acquire lock...", serviceName);
              servicesBlocked.incrementAndGet();
              BootstrapStateHandler.Lock serviceLock = service.getBootstrapStateLock();
              try (UncheckedAutoCloseable lock = serviceLock.acquireReadLock()) {
                // Should block!
                servicesBlocked.decrementAndGet();
                servicesSucceeded.incrementAndGet();
                LOG.info(" {} : Lock acquired!", serviceName);
              }
            }
            allServicesCompleted.countDown();
          } catch (Exception e) {
            LOG.error("{}  failed", serviceName, e);
            allServicesCompleted.countDown();
          }
        });
    // Start all threads
    checkpointThread.start();
    Thread keyDeletingThread = createServiceThread.apply("KeyDeletingService",
        om.getKeyManager().getDeletingService());
    Thread dirDeletingThread = createServiceThread.apply("DirectoryDeletingService",
        om.getKeyManager().getDirDeletingService());
    Thread snapshotDeletingThread = createServiceThread.apply("SnapshotDeletingService",
        om.getKeyManager().getSnapshotDeletingService());
    keyDeletingThread.start();
    dirDeletingThread.start();
    snapshotDeletingThread.start();
    // Wait a bit, then verify multiple services are blocked
    Thread.sleep(1000);
    int blockedCount = servicesBlocked.get();
    assertTrue(blockedCount > 0, "At least one service should be blocked");
    assertEquals(0, servicesSucceeded.get(), "No services should have succeeded yet");
    // Wait for completion
    assertTrue(allServicesCompleted.await(10, TimeUnit.SECONDS));
    // Verify all services eventually succeeded
    assertEquals(0, servicesBlocked.get(), "No services should be blocked anymore");
    assertTrue(servicesSucceeded.get() > 0, "Services should have succeeded after lock release");
  }

  /**
   * Tests the full checkpoint servlet flow to ensure snapshot paths are read
   * from checkpoint metadata (frozen state) rather than live OM metadata (current state).
   * Scenario:
   * 1. Create snapshots S1
   * 2. create snapshot S2 later just before checkpoint
   * 3. Servlet processes checkpoint - should still include S1, S3 data as
   *    checkpoint snapshotInfoTable has S1 S3
   */
  @Test
  public void testCheckpointIncludesSnapshotsFromFrozenState() throws Exception {
    String volumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck" + RandomStringUtils.secure().nextNumeric(5);

    setupCluster();
    om.getKeyManager().getSnapshotSstFilteringService().pause();

    // Create test data and snapshots
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName);

    // Create key before first snapshot
    TestDataUtil.createKey(bucket, "key1",
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
        "data1".getBytes(StandardCharsets.UTF_8));
    client.getObjectStore().createSnapshot(volumeName, bucketName, "snapshot1");
    // At this point: Live OM has snapshots S1
    List<OzoneSnapshot> snapshots = new ArrayList<>();
    client.getObjectStore().listSnapshot(volumeName, bucketName, "", null)
        .forEachRemaining(snapshots::add);
    assertEquals(1, snapshots.size(), "Should have 1 snapshot initially");
    OzoneSnapshot snapshot1 = snapshots.stream()
        .filter(snap -> snap.getName().equals("snapshot1"))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("snapshot1 not found"));
    // Setup servlet mocks for checkpoint processing
    setupMocks();
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA)).thenReturn("true");

    // Create a checkpoint that captures current state (S1)
    DBStore spyDbStore = spy(om.getMetadataManager().getStore());

    AtomicReference<DBCheckpoint> capturedCheckpoint = new AtomicReference<>();
    SnapshotCache spySnapshotCache = spy(om.getOmSnapshotManager().getSnapshotCache());
    OmSnapshotManager spySnapshotManager = spy(om.getOmSnapshotManager());
    when(om.getOmSnapshotManager()).thenReturn(spySnapshotManager);
    when(spySnapshotManager.getSnapshotCache()).thenReturn(spySnapshotCache);
    // Mock the snapshot cache to create a snapshot2 just after taking a snapshot cache lock.
    doAnswer(invocationOnMock -> {
      Object ret = invocationOnMock.callRealMethod();
      // create snapshot 3 before checkpoint
      client.getObjectStore().createSnapshot(volumeName, bucketName, "snapshot2");
      // Also wait for double buffer to flush to ensure all transactions are committed
      om.awaitDoubleBufferFlush();
      return ret;
    }).when(spySnapshotCache).lock();
    doAnswer(invocation -> {
      DBCheckpoint checkpoint = (DBCheckpoint) spy(invocation.callRealMethod());
      doNothing().when(checkpoint).cleanupCheckpoint(); // Don't cleanup for verification
      capturedCheckpoint.set(checkpoint);
      return checkpoint;
    }).when(spyDbStore).getCheckpoint(eq(true));

    // Initialize servlet
    doCallRealMethod().when(omDbCheckpointServletMock).initialize(any(), any(),
        eq(false), any(), any(), eq(false));
    omDbCheckpointServletMock.initialize(spyDbStore, om.getMetrics().getDBCheckpointMetrics(),
        false, om.getOmAdminUsernames(), om.getOmAdminGroups(), false);
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);
    // Process checkpoint servlet
    omDbCheckpointServletMock.doGet(requestMock, responseMock);
    snapshots.clear();
    client.getObjectStore().listSnapshot(volumeName, bucketName, "", null)
        .forEachRemaining(snapshots::add);
    assertEquals(2, snapshots.size(), "Should have 2 snapshots");
    OzoneSnapshot snapshot2 = snapshots.stream()
        .filter(snap -> snap.getName().equals("snapshot2"))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("snapshot2 not found"));
    // Extract tarball and verify contents
    String testDirName = folder.resolve("testDir").toString();
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    File newDbDir = new File(newDbDirName);
    assertTrue(newDbDir.mkdirs());
    FileUtil.unTar(tempFile, newDbDir);
    InodeMetadataRocksDBCheckpoint obtainedCheckpoint =
        new InodeMetadataRocksDBCheckpoint(newDbDir.toPath());
    assertNotNull(obtainedCheckpoint);
    Path snapshot1DbDir = Paths.get(newDbDir.getPath(), OM_SNAPSHOT_CHECKPOINT_DIR,
        OM_DB_NAME + "-" + snapshot1.getSnapshotId());
    Path snapshot2DbDir = Paths.get(newDbDir.getPath(),  OM_SNAPSHOT_CHECKPOINT_DIR,
        OM_DB_NAME + "-" + snapshot2.getSnapshotId());
    boolean snapshot1IncludedInCheckpoint = Files.exists(snapshot1DbDir);
    boolean snapshot2IncludedInCheckpoint = Files.exists(snapshot2DbDir);
    assertTrue(snapshot1IncludedInCheckpoint && snapshot2IncludedInCheckpoint,
        "Checkpoint should include both snapshot1 and snapshot2 data");
    // Cleanup
    if (capturedCheckpoint.get() != null) {
      capturedCheckpoint.get().cleanupCheckpoint();
    }
  }

  private void writeDummyKeyToDeleteTableOfSnapshotDB(OzoneSnapshot snapshotToModify, String bucketName,
      String volumeName, String keyName)
      throws IOException {
    try (UncheckedAutoCloseableSupplier<OmSnapshot> supplier = om.getOmSnapshotManager()
        .getSnapshot(snapshotToModify.getSnapshotId())) {
      OmSnapshot omSnapshot = supplier.get();
      OmKeyInfo dummyOmKeyInfo =
          new OmKeyInfo.Builder().setBucketName(bucketName).setVolumeName(volumeName).setKeyName(keyName)
              .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE)).build();
      RepeatedOmKeyInfo dummyRepeatedKeyInfo =
          new RepeatedOmKeyInfo.Builder().setOmKeyInfos(Collections.singletonList(dummyOmKeyInfo)).build();
      omSnapshot.getMetadataManager().getDeletedTable().put(dummyOmKeyInfo.getKeyName(), dummyRepeatedKeyInfo);
    }
  }

  private void setupClusterAndMocks(String volumeName, String bucketName,
      AtomicReference<DBCheckpoint> realCheckpoint, boolean includeSnapshots) throws Exception {
    setupCluster();
    setupMocks();
    om.getKeyManager().getSnapshotSstFilteringService().pause();
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn(String.valueOf(includeSnapshots));
    // Create a "spy" dbstore keep track of the checkpoint.
    writeData(volumeName, bucketName, true);
    DBStore dbStore = om.getMetadataManager().getStore();
    DBStore spyDbStore = spy(dbStore);
    when(spyDbStore.getCheckpoint(true)).thenAnswer(b -> {
      DBCheckpoint checkpoint = spy(dbStore.getCheckpoint(true));
      // Don't delete the checkpoint, because we need to compare it
      // with the snapshot data.
      doNothing().when(checkpoint).cleanupCheckpoint();
      realCheckpoint.set(checkpoint);
      return checkpoint;
    });
    // Init the mock with the spyDbstore
    doCallRealMethod().when(omDbCheckpointServletMock).initialize(any(), any(),
        eq(false), any(), any(), eq(false));
    doCallRealMethod().when(omDbCheckpointServletMock).getSnapshotDirsFromDB(any(), any(), any());
    doCallRealMethod().when(omDbCheckpointServletMock).collectDbDataToTransfer(any(), any(), any());
    omDbCheckpointServletMock.initialize(spyDbStore, om.getMetrics().getDBCheckpointMetrics(),
        false,
        om.getOmAdminUsernames(), om.getOmAdminGroups(), false);
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);
  }

  String getValueFromSnapshotDeleteTable(String key, String snapshotDB) {
    String result = null;
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    int count = 1;
    int deletedTableCFIndex = 0;
    cfDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(StandardCharsets.UTF_8)));
    for (String cfName : OMDBDefinition.getAllColumnFamilies()) {
      if (cfName.equals(OMDBDefinition.DELETED_TABLE)) {
        deletedTableCFIndex = count;
      }
      cfDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes(StandardCharsets.UTF_8)));
      count++;
    }
    // For holding handles
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    try (DBOptions options = new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(true);
        RocksDB db = RocksDB.openReadOnly(options, snapshotDB, cfDescriptors, cfHandles)) {

      ColumnFamilyHandle deletedTableCF = cfHandles.get(deletedTableCFIndex); // 0 is default
      byte[] value = db.get(deletedTableCF, key.getBytes(StandardCharsets.UTF_8));
      if (value != null) {
        result = new String(value, StandardCharsets.UTF_8);
      }
    } catch (Exception e) {
      fail("Exception while reading from snapshot DB " + e.getMessage());
    } finally {
      for (ColumnFamilyHandle handle : cfHandles) {
        handle.close();
      }
    }
    return result;
  }

  public static Map<String, List<String>> readFileToMap(String filePath) throws IOException {
    Map<String, List<String>> dataMap = new HashMap<>();
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String trimmedLine = line.trim();
        if (!trimmedLine.contains("\t")) {
          continue;
        }
        int tabIndex = trimmedLine.indexOf("\t");
        if (tabIndex > 0) {
          // value is the full path that needs to be constructed
          String value = trimmedLine.substring(0, tabIndex).trim();
          // key is the inodeID
          String key = getInode(trimmedLine.substring(tabIndex + 1).trim());
          if (!key.isEmpty() && !value.isEmpty()) {
            dataMap.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
          }
        }
      }
    }
    for (Map.Entry<String, List<String>> entry : dataMap.entrySet()) {
      Collections.sort(entry.getValue());
    }
    return dataMap;
  }

  private  void populateInodesOfFilesInDirectory(DBStore dbStore, Path dbLocation,
      Set<String> inodesFromOmDbCheckpoint, Map<String, List<String>> hardlinkMap) throws IOException {
    try (Stream<Path> filesInOmDb = Files.list(dbLocation)) {
      List<Path> files = filesInOmDb.collect(Collectors.toList());
      for (Path p : files) {
        if (Files.isDirectory(p) || p.toFile().getName().equals(OmSnapshotManager.OM_HARDLINK_FILE)) {
          continue;
        }
        String inode = getInode(OmSnapshotUtils.getFileInodeAndLastModifiedTimeString(p));
        Path metadataDir = OMStorage.getOmDbDir(conf).toPath();
        String path  = metadataDir.relativize(p).toString();
        if (path.contains(OM_CHECKPOINT_DIR)) {
          path = metadataDir.relativize(dbStore.getDbLocation().toPath().resolve(p.getFileName())).toString();
        }
        hardlinkMap.computeIfAbsent(inode, k -> new ArrayList<>()).add(path);
        inodesFromOmDbCheckpoint.add(inode);
      }
    }
    for (Map.Entry<String, List<String>> entry : hardlinkMap.entrySet()) {
      Collections.sort(entry.getValue());
    }
  }

  private String getSnapshotDBPath(String checkPointDir) {
    return OMStorage.getOmDbDir(cluster.getConf()) +
        OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX +
        OM_DB_NAME + checkPointDir;
  }

  private static String getInode(String inodeAndMtime) {
    String inode = inodeAndMtime.split("-")[0];
    return inode;
  }

  private void writeData(String volumeName, String bucketName, boolean includeSnapshots) throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName);
    for (int i = 0; i < 10; i++) {
      TestDataUtil.createKey(bucket, "key" + i,
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
          "sample".getBytes(StandardCharsets.UTF_8));
      om.getMetadataManager().getStore().flushDB();
    }
    if (includeSnapshots) {
      TestDataUtil.createKey(bucket, "keysnap1",
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
          "sample".getBytes(StandardCharsets.UTF_8));
      TestDataUtil.createKey(bucket, "keysnap2",
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
          "sample".getBytes(StandardCharsets.UTF_8));
      client.getObjectStore().createSnapshot(volumeName, bucketName, "snapshot10");
      client.getObjectStore().createSnapshot(volumeName, bucketName, "snapshot20");
    }
  }
}
