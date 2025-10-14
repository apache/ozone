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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.Archiver;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
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
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
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
  }

  @AfterEach
  void shutdown() {
    IOUtils.closeQuietly(client, cluster);
  }

  private void setupCluster() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    om = cluster.getOzoneManager();
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
        .writeDBToArchive(any(), any(), any(), any(), any(), any(), anyBoolean());

    when(omDbCheckpointServletMock.getBootstrapStateLock())
        .thenReturn(lock);
    doCallRealMethod().when(omDbCheckpointServletMock).getCheckpoint(any(), anyBoolean());
    assertNull(doCallRealMethod().when(omDbCheckpointServletMock).getBootstrapTempData());
    doCallRealMethod().when(omDbCheckpointServletMock).getSnapshotDirs(any());
    doCallRealMethod().when(omDbCheckpointServletMock).
        processMetadataSnapshotRequest(any(), any(), anyBoolean(), anyBoolean());
    doCallRealMethod().when(omDbCheckpointServletMock).writeDbDataToStream(any(), any(), any(), any());
    doCallRealMethod().when(omDbCheckpointServletMock).getCompactionLogDir();
    doCallRealMethod().when(omDbCheckpointServletMock).getSstBackupDir();
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

    // create hardlinks now
    OmSnapshotUtils.createHardLinks(newDbDir.toPath(), true);

    if (includeSnapshot) {
      List<String> yamlRelativePaths = snapshotPaths.stream().map(path -> {
        int startIndex = path.indexOf("db.snapshots");
        if (startIndex != -1) {
          return path.substring(startIndex) + ".yaml";
        }
        return path + ".yaml";
      }).collect(Collectors.toList());

      for (String yamlRelativePath : yamlRelativePaths) {
        String yamlFileName = Paths.get(newDbDir.getPath(), yamlRelativePath).toString();
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
    Set<Path> allPathsInTarball = getAllPathsInTarball(newDbDir);
    // create hardlinks now
    OmSnapshotUtils.createHardLinks(newDbDir.toPath(), false);
    for (Path old : allPathsInTarball) {
      assertTrue(old.toFile().delete());
    }
    Path snapshotDbDir = Paths.get(newDbDir.toPath().toString(), OM_SNAPSHOT_CHECKPOINT_DIR,
        OM_DB_NAME + "-" + snapshotToModify.getSnapshotId());
    deleteWalFiles(snapshotDbDir);
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
    Map<String, String> hardLinkFileMap = new java.util.HashMap<>();
    Path tmpDir = folder.resolve("tmp");
    Files.createDirectories(tmpDir);
    TarArchiveOutputStream mockArchiveOutputStream = mock(TarArchiveOutputStream.class);
    List<String> fileNames = new ArrayList<>();
    try (MockedStatic<Archiver> archiverMock = mockStatic(Archiver.class)) {
      archiverMock.when(() -> Archiver.linkAndIncludeFile(any(), any(), any(), any())).thenAnswer(invocation -> {
        // Get the actual mockArchiveOutputStream passed from writeDBToArchive
        TarArchiveOutputStream aos = invocation.getArgument(2);
        File sourceFile = invocation.getArgument(0);
        String fileId = invocation.getArgument(1);
        fileNames.add(sourceFile.getName());
        aos.putArchiveEntry(new TarArchiveEntry(sourceFile, fileId));
        aos.write(new byte[100], 0, 100); // Simulate writing
        aos.closeArchiveEntry();
        return 100L;
      });
      boolean success = omDbCheckpointServletMock.writeDBToArchive(
          sstFilesToExclude, dbDir, maxTotalSstSize, mockArchiveOutputStream,
              tmpDir, hardLinkFileMap, expectOnlySstFiles);
      assertTrue(success);
      verify(mockArchiveOutputStream, times(fileNames.size())).putArchiveEntry(any());
      verify(mockArchiveOutputStream, times(fileNames.size())).closeArchiveEntry();
      verify(mockArchiveOutputStream, times(fileNames.size())).write(any(byte[].class), anyInt(),
          anyInt()); // verify write was called once

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
  }

  @Test
  public void testBootstrapLockCoordination() throws Exception {
    // Create mocks for all background services
    KeyDeletingService mockDeletingService = mock(KeyDeletingService.class);
    DirectoryDeletingService mockDirDeletingService = mock(DirectoryDeletingService.class);
    SstFilteringService mockFilteringService = mock(SstFilteringService.class);
    SnapshotDeletingService mockSnapshotDeletingService = mock(SnapshotDeletingService.class);
    RocksDBCheckpointDiffer mockCheckpointDiffer = mock(RocksDBCheckpointDiffer.class);
    // Create mock locks for each service
    BootstrapStateHandler.Lock mockDeletingLock = mock(BootstrapStateHandler.Lock.class);
    BootstrapStateHandler.Lock mockDirDeletingLock = mock(BootstrapStateHandler.Lock.class);
    BootstrapStateHandler.Lock mockFilteringLock = mock(BootstrapStateHandler.Lock.class);
    BootstrapStateHandler.Lock mockSnapshotDeletingLock = mock(BootstrapStateHandler.Lock.class);
    BootstrapStateHandler.Lock mockCheckpointDifferLock = mock(BootstrapStateHandler.Lock.class);
    // Configure service mocks to return their respective locks
    when(mockDeletingService.getBootstrapStateLock()).thenReturn(mockDeletingLock);
    when(mockDirDeletingService.getBootstrapStateLock()).thenReturn(mockDirDeletingLock);
    when(mockFilteringService.getBootstrapStateLock()).thenReturn(mockFilteringLock);
    when(mockSnapshotDeletingService.getBootstrapStateLock()).thenReturn(mockSnapshotDeletingLock);
    when(mockCheckpointDiffer.getBootstrapStateLock()).thenReturn(mockCheckpointDifferLock);
    // Mock KeyManager and its services
    KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.getDeletingService()).thenReturn(mockDeletingService);
    when(mockKeyManager.getDirDeletingService()).thenReturn(mockDirDeletingService);
    when(mockKeyManager.getSnapshotSstFilteringService()).thenReturn(mockFilteringService);
    when(mockKeyManager.getSnapshotDeletingService()).thenReturn(mockSnapshotDeletingService);
    // Mock OMMetadataManager and Store
    OMMetadataManager mockMetadataManager = mock(OMMetadataManager.class);
    DBStore mockStore = mock(DBStore.class);
    when(mockMetadataManager.getStore()).thenReturn(mockStore);
    when(mockStore.getRocksDBCheckpointDiffer()).thenReturn(mockCheckpointDiffer);
    // Mock OzoneManager
    OzoneManager mockOM = mock(OzoneManager.class);
    when(mockOM.getKeyManager()).thenReturn(mockKeyManager);
    when(mockOM.getMetadataManager()).thenReturn(mockMetadataManager);
    // Create the actual Lock instance (this tests the real implementation)
    OMDBCheckpointServlet.Lock bootstrapLock = new OMDBCheckpointServlet.Lock(mockOM);
    // Test successful lock acquisition
    BootstrapStateHandler.Lock result = bootstrapLock.lock();
    // Verify all service locks were acquired
    verify(mockDeletingLock).lock();
    verify(mockDirDeletingLock).lock();
    verify(mockFilteringLock).lock();
    verify(mockSnapshotDeletingLock).lock();
    verify(mockCheckpointDifferLock).lock();
    // Verify double buffer flush was called
    verify(mockOM).awaitDoubleBufferFlush();
    // Verify the lock returns itself
    assertEquals(bootstrapLock, result);
    // Test unlock
    bootstrapLock.unlock();
    // Verify all service locks were released
    verify(mockDeletingLock).unlock();
    verify(mockDirDeletingLock).unlock();
    verify(mockFilteringLock).unlock();
    verify(mockSnapshotDeletingLock).unlock();
    verify(mockCheckpointDifferLock).unlock();
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
      try {
        LOG.info("Acquiring bootstrap lock for checkpoint...");
        BootstrapStateHandler.Lock acquired = bootstrapLock.lock();
        bootstrapAcquired.countDown();
        Thread.sleep(3000); // Hold for 3 seconds
        LOG.info("Releasing bootstrap lock...");
        acquired.unlock();
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
              serviceLock.lock(); // Should block!
              servicesBlocked.decrementAndGet();
              servicesSucceeded.incrementAndGet();
              LOG.info(" {} : Lock acquired!", serviceName);
              serviceLock.unlock();
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

  private static void deleteWalFiles(Path snapshotDbDir) throws IOException {
    try (Stream<Path> filesInTarball = Files.list(snapshotDbDir)) {
      List<Path> files = filesInTarball.filter(p -> p.toString().contains(".log"))
          .collect(Collectors.toList());
      for (Path p : files) {
        Files.delete(p);
      }
    }
  }

  private static Set<Path> getAllPathsInTarball(File newDbDir) throws IOException {
    Set<Path> allPathsInTarball = new HashSet<>();
    try (Stream<Path> filesInTarball = Files.list(newDbDir.toPath())) {
      List<Path> files = filesInTarball.collect(Collectors.toList());
      for (Path p : files) {
        File file = p.toFile();
        if (file.getName().equals(OmSnapshotManager.OM_HARDLINK_FILE)) {
          continue;
        }
        allPathsInTarball.add(p);
      }
    }
    return allPathsInTarball;
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
        if (path.startsWith(OM_DB_NAME)) {
          Path fileName = Paths.get(path).getFileName();
          // fileName will not be null, added null check for findbugs
          if (fileName != null) {
            path = fileName.toString();
          }
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
