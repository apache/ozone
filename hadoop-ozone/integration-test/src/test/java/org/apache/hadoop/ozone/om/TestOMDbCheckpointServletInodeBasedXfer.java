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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  @BeforeEach
  void init() throws Exception {
    conf = new OzoneConfiguration();
  }

  @AfterEach
  void shutdown() {
    IOUtils.closeQuietly(client, cluster);
  }

  private void setupCluster() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    om = cluster.getOzoneManager();
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
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

    BootstrapStateHandler.Lock lock = new OMDBCheckpointServlet.Lock(om);
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

  @Test
  void testContentsOfTarballWithSnapshot() throws Exception {
    setupCluster();
    setupMocks();
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA)).thenReturn("true");
    String volumeName = "vol" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck" + RandomStringUtils.secure().nextNumeric(5);
    // Create a "spy" dbstore keep track of the checkpoint.
    writeData(volumeName, bucketName, true);
    DBStore dbStore = om.getMetadataManager().getStore();
    DBStore spyDbStore = spy(dbStore);
    AtomicReference<DBCheckpoint> realCheckpoint = new AtomicReference<>();
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

    // Get the tarball.
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);
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
    for (String snapshotPath : snapshotPaths) {
      populateInodesOfFilesInDirectory(dbStore, Paths.get(snapshotPath),
          inodesFromOmDataDir, hardLinkMapFromOmData);
    }
    Map<String, List<String>> hardlinkMapFromTarball = readFileToMap(newDbDir.toPath()
        .resolve(OmSnapshotManager.OM_HARDLINK_FILE).toString());

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
  }

  public static Map<String, List<String>> readFileToMap(String filePath) throws IOException {
    Map<String, List<String>> dataMap = new HashMap<>();
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String trimmedLine = line.trim();
        if (trimmedLine.isEmpty() || !trimmedLine.contains("\t")) {
          continue;
        }
        int tabIndex = trimmedLine.indexOf("\t");
        if (tabIndex > 0) {
          String key = getInode(trimmedLine.substring(0, tabIndex).trim());
          // Value starts after the tab, trimming any extra whitespace
          String value = trimmedLine.substring(tabIndex + 1).trim();
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
        String inode = getInode(OmSnapshotUtils.getInodeAndMtime(p));
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
