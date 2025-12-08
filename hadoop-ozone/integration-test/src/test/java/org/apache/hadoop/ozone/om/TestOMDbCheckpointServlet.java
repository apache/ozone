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

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.MULTIPART_FORM_DATA_BOUNDARY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_PREFIX;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_SUFFIX;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.truncateFileName;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COMPACTION_LOG_FILE_NAME_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.ServletContext;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Class used for testing the OM DB Checkpoint provider servlet.
 */
public class TestOMDbCheckpointServlet {
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
  private OzoneConfiguration conf;
  private File tempFile;
  private ServletOutputStream servletOutputStream;
  private MiniOzoneCluster cluster = null;
  private OzoneClient client;
  private OzoneManager om;
  private OMMetrics omMetrics = null;
  private HttpServletRequest requestMock = null;
  private HttpServletResponse responseMock = null;
  private OMDBCheckpointServlet omDbCheckpointServletMock = null;
  private File metaDir;
  private String snapshotDirName;
  private String snapshotDirName2;
  private Path compactionDirPath;
  private DBCheckpoint dbCheckpoint;
  @TempDir
  private Path folder;
  private static final String FABRICATED_FILE_NAME = "fabricatedFile.sst";
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
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    om = cluster.getOzoneManager();
    omMetrics = om.getMetrics();
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

    omDbCheckpointServletMock =
        mock(OMDBCheckpointServlet.class);

    BootstrapStateHandler.Lock lock =
        new OMDBCheckpointServlet.Lock(om);
    doCallRealMethod().when(omDbCheckpointServletMock).init();
    assertNull(
        doCallRealMethod().when(omDbCheckpointServletMock).getDbStore());

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

    doCallRealMethod().when(omDbCheckpointServletMock).getCheckpoint(any(),
        anyBoolean());
    doCallRealMethod().when(omDbCheckpointServletMock)
        .processMetadataSnapshotRequest(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  void testWithoutACL() throws Exception {
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);

    setupCluster();

    testBootstrapLocking();

    testEndpoint("POST");
    testEndpoint("GET");
    testDoPostWithInvalidContentType();

    prepSnapshotData();

    testWriteDbDataWithoutOmSnapshot();
    testWriteDbDataToStream();
    testWriteDbDataWithToExcludeFileList();
  }

  private void testEndpoint(String method) throws Exception {
    setupMocks();

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        om.getOmAdminUsernames(),
        om.getOmAdminGroups(),
        om.isSpnegoEnabled());

    doNothing().when(responseMock).setContentType("application/x-tar");
    doNothing().when(responseMock).setHeader(anyString(), anyString());

    Set<String> toExcludeList = new HashSet<>();
    toExcludeList.add("sstFile1.sst");
    toExcludeList.add("sstFile2.sst");

    setupHttpMethod(method, toExcludeList);

    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.init();
    long initialCheckpointCount =
        omMetrics.getDBCheckpointMetrics().getNumCheckpoints();

    doEndpoint(method);

    assertThat(tempFile.length()).isGreaterThan(0);
    assertThat(omMetrics.getDBCheckpointMetrics().getLastCheckpointCreationTimeTaken())
        .isGreaterThan(0);
    assertThat(omMetrics.getDBCheckpointMetrics().getLastCheckpointStreamingTimeTaken())
        .isGreaterThan(0);
    assertThat(omMetrics.getDBCheckpointMetrics().getNumCheckpoints())
        .isGreaterThan(initialCheckpointCount);

    verify(omDbCheckpointServletMock).writeDbDataToStream(any(),
        any(), any(), eq(toExcludeList), any());
  }

  private void testDoPostWithInvalidContentType() throws Exception {
    setupMocks();

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        om.getOmAdminUsernames(),
        om.getOmAdminGroups(),
        om.isSpnegoEnabled());

    when(requestMock.getContentType()).thenReturn("application/json");
    doNothing().when(responseMock).setContentType("application/x-tar");
    doNothing().when(responseMock).setHeader(anyString(),
        anyString());

    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.init();
    omDbCheckpointServletMock.doPost(requestMock, responseMock);

    verify(responseMock).setStatus(HttpServletResponse.SC_BAD_REQUEST);
  }

  @Test
  void testSpnegoEnabled() throws Exception {
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, "");
    conf.set(OZONE_OM_HTTP_AUTH_TYPE, "kerberos");
    conf.set(OZONE_RECON_KERBEROS_PRINCIPAL_KEY, "recon/host1@REALM");

    setupCluster();

    testSpnegoEnabled("POST");
    testSpnegoEnabled("GET");
  }

  private void testSpnegoEnabled(String method) throws Exception {
    setupMocks();

    Collection<String> allowedUsers =
            new LinkedHashSet<>(om.getOmAdminUsernames());
    allowedUsers.add("recon");

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        allowedUsers,
        Collections.emptySet(),
        om.isSpnegoEnabled());

    omDbCheckpointServletMock.init();

    setupHttpMethod(method, new ArrayList<>());

    doEndpoint(method);

    // Response status should be set to 403 Forbidden since there was no user
    // principal set in the request
    verify(responseMock, times(1)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Set the principal to DN in request
    // This should also get denied since only OM and recon
    // users should be granted access to the servlet
    Principal userPrincipalMock = mock(Principal.class);
    when(userPrincipalMock.getName()).thenReturn("dn/localhost@REALM");
    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);

    doEndpoint(method);

    // Verify that the Response status is set to 403 again for DN user.
    verify(responseMock, times(2)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Now, set the principal to recon in request
    when(userPrincipalMock.getName()).thenReturn("recon/localhost@REALM");

    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    doEndpoint(method);

    // Recon user should be able to access the servlet and download the
    // snapshot
    assertThat(tempFile.length()).isGreaterThan(0);
  }

  private void testWriteDbDataToStream() throws Exception {
    setupMocks();

    // Set http param to include snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn("true");

    // Create a "spy" dbstore keep track of the checkpoint.
    DBStore dbStore =  om.getMetadataManager().getStore();
    DBStore spyDbStore = spy(dbStore);

    int metaDirLength = metaDir.toString().length() + 1;
    String compactionLogDir = dbStore.
        getRocksDBCheckpointDiffer().getCompactionLogDir();
    String sstBackupDir = dbStore.
        getRocksDBCheckpointDiffer().getSSTBackupDir();

    // Create files to be copied from the compaction pause
    // temp directories so we can confirm they are correctly
    // copied.  The unexpected files should NOT be copied.
    Path expectedLog = Paths.get(compactionLogDir, "expected" +
        COMPACTION_LOG_FILE_NAME_SUFFIX);
    String expectedLogStr = truncateFileName(metaDirLength, expectedLog);
    Path expectedSst = Paths.get(sstBackupDir, "expected.sst");
    String expectedSstStr = truncateFileName(metaDirLength, expectedSst);

    // put "expected" fabricated files onto the fs before the files get
    //  copied to the temp dir.
    Files.write(expectedLog,
        "fabricatedData".getBytes(StandardCharsets.UTF_8));
    Files.write(expectedSst,
        "fabricatedData".getBytes(StandardCharsets.UTF_8));

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
    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        any(), any(), eq(false), any(), any(), eq(false));
    omDbCheckpointServletMock.initialize(
        spyDbStore, om.getMetrics().getDBCheckpointMetrics(),
        false, om.getOmAdminUsernames(), om.getOmAdminGroups(), false);

    // Get the tarball.
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);
    long tmpHardLinkFileCount = tmpHardLinkFileCount();
    omDbCheckpointServletMock.doGet(requestMock, responseMock);
    assertEquals(tmpHardLinkFileCount, tmpHardLinkFileCount());
    dbCheckpoint = realCheckpoint.get();

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.resolve("testDir").toString();
    int testDirLength = testDirName.length() + 1;
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    int newDbDirLength = newDbDirName.length() + 1;
    File newDbDir = new File(newDbDirName);
    assertTrue(newDbDir.mkdirs());
    FileUtil.unTar(tempFile, newDbDir);

    // Move snapshot dir to correct location.
    assertTrue(new File(newDbDirName, OM_SNAPSHOT_DIR)
        .renameTo(new File(newDbDir.getParent(), OM_SNAPSHOT_DIR)));

    // Confirm the checkpoint directories match, (after remove extras).
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Path finalCheckpointLocation = Paths.get(newDbDirName);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        newDbDirLength);

    assertThat(finalCheckpointSet).withFailMessage("hardlink file exists in checkpoint dir")
        .contains(OM_HARDLINK_FILE);
    finalCheckpointSet.remove(OM_HARDLINK_FILE);
    assertEquals(initialCheckpointSet, finalCheckpointSet);

    String shortSnapshotLocation =
        truncateFileName(metaDirLength, Paths.get(snapshotDirName));
    String shortSnapshotLocation2 =
        truncateFileName(metaDirLength, Paths.get(snapshotDirName2));
    String shortCompactionDirLocation =
        truncateFileName(metaDirLength, compactionDirPath);

    Set<String> finalFullSet =
        getFiles(Paths.get(testDirName, OM_SNAPSHOT_DIR), testDirLength);

    // Check each line in the hard link file.
    List<String> fabricatedLinkLines = new ArrayList<>();
    try (Stream<String> lines = Files.lines(Paths.get(newDbDirName,
        OM_HARDLINK_FILE))) {

      for (String line : lines.collect(Collectors.toList())) {
        assertFalse(line.contains("CURRENT"),
            "CURRENT file is not a hard link");
        if (line.contains(FABRICATED_FILE_NAME)) {
          fabricatedLinkLines.add(line);
        } else {
          checkLine(shortSnapshotLocation, shortSnapshotLocation2, line);
          // add links to the final set
          finalFullSet.add(line.split("\t")[0]);
        }
      }
    }
    Set<String> directories = Sets.newHashSet(
        shortSnapshotLocation, shortSnapshotLocation2,
        shortCompactionDirLocation);
    checkFabricatedLines(directories, fabricatedLinkLines, testDirName);

    Set<String> initialFullSet =
        getFiles(Paths.get(metaDir.toString(), OM_SNAPSHOT_DIR), metaDirLength);
    assertThat(finalFullSet).contains(expectedLogStr);
    assertThat(finalFullSet).contains(expectedSstStr);
    assertEquals(initialFullSet, finalFullSet, "expected snapshot files not found");
  }

  private static long tmpHardLinkFileCount() throws IOException {
    Path tmpDirPath = Paths.get(System.getProperty(JAVA_IO_TMPDIR));
    try (Stream<Path> tmpFiles = Files.list(tmpDirPath)) {
      return tmpFiles
          .filter(path -> {
            String regex = DATA_PREFIX + ".*" + DATA_SUFFIX;
            return path.getFileName().toString().matches(regex);
          })
          .count();
    }
  }

  private void testWriteDbDataWithoutOmSnapshot()
      throws Exception {
    setupMocks();

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        any(), any(), anyBoolean(), any(), any(), anyBoolean());
    omDbCheckpointServletMock.init();

    // Set http param to exclude snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn(null);

    // Get the tarball.
    Path tmpdir = folder.resolve("bootstrapData");
    try (OutputStream fileOutputStream = Files.newOutputStream(tempFile.toPath())) {
      omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint, requestMock,
          fileOutputStream, new HashSet<>(), tmpdir);
    }

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.resolve("testDir").toString();
    int testDirLength = testDirName.length() + 1;
    FileUtil.unTar(tempFile, new File(testDirName));

    // Confirm the checkpoint directories match.
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Path finalCheckpointLocation = Paths.get(testDirName);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        testDirLength);

    assertEquals(initialCheckpointSet, finalCheckpointSet);
  }

  private void testWriteDbDataWithToExcludeFileList()
      throws Exception {
    setupMocks();

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        any(), any(), anyBoolean(), any(), any(), anyBoolean());
    omDbCheckpointServletMock.init();

    File dummyFile = new File(dbCheckpoint.getCheckpointLocation().toString(),
        "dummy.sst");
    try (OutputStreamWriter writer = new OutputStreamWriter(
        Files.newOutputStream(dummyFile.toPath()), StandardCharsets.UTF_8)) {
      writer.write("Dummy data.");
    }
    assertTrue(dummyFile.exists());
    Set<String> toExcludeList = new HashSet<>();
    toExcludeList.add(dummyFile.getName());

    // Set http param to exclude snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn(null);

    // Get the tarball.
    Path tmpdir = folder.resolve("bootstrapData");
    try (OutputStream fileOutputStream = Files.newOutputStream(tempFile.toPath())) {
      omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint, requestMock,
          fileOutputStream, toExcludeList, tmpdir);
    }

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.resolve("testDir").toString();
    int testDirLength = testDirName.length() + 1;
    FileUtil.unTar(tempFile, new File(testDirName));

    // Confirm the checkpoint directories match.
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Path finalCheckpointLocation = Paths.get(testDirName);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        testDirLength);

    initialCheckpointSet.removeAll(finalCheckpointSet);
    assertThat(initialCheckpointSet).contains(dummyFile.getName());
  }

  /**
   * Calls endpoint in regards to parametrized HTTP method.
   */
  private void doEndpoint(String method) {
    if (method.equals("POST")) {
      omDbCheckpointServletMock.doPost(requestMock, responseMock);
    } else {
      omDbCheckpointServletMock.doGet(requestMock, responseMock);
    }
  }

  /**
   * Setups HTTP method details depending on parametrized HTTP method.
   *
   * @param toExcludeList SST file names to be excluded.
   * @throws IOException
   */
  private void setupHttpMethod(String method, Collection <String> toExcludeList) throws IOException {
    if (method.equals("POST")) {
      setupPostMethod(toExcludeList);
    } else {
      setupGetMethod(toExcludeList);
    }
  }

  /**
   * Setups details for HTTP POST request.
   * @param toExcludeList SST file names to be excluded.
   * @throws IOException
   */
  private void setupPostMethod(Collection<String> toExcludeList)
      throws IOException {
    when(requestMock.getMethod()).thenReturn("POST");
    when(requestMock.getContentType()).thenReturn("multipart/form-data; " +
        "boundary=" + MULTIPART_FORM_DATA_BOUNDARY);

    // Generate form data
    String crNl = "\r\n";
    String contentDisposition = "Content-Disposition: form-data; name=\"" +
        OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST + "[]\"" + crNl + crNl;
    String boundary = "--" + MULTIPART_FORM_DATA_BOUNDARY;
    String endBoundary = boundary + "--" + crNl;
    StringBuilder sb = new StringBuilder();
    toExcludeList.forEach(sfn -> {
      sb.append(boundary).append(crNl);
      sb.append(contentDisposition);
      sb.append(sfn).append(crNl);
    });
    sb.append(endBoundary);

    // Use generated form data as input stream to the HTTP request
    InputStream input = new ByteArrayInputStream(
        sb.toString().getBytes(StandardCharsets.UTF_8));
    ServletInputStream inputStream = mock(ServletInputStream.class);
    when(requestMock.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(byte[].class), anyInt(), anyInt()))
        .thenAnswer(invocation -> {
          byte[] buffer = invocation.getArgument(0);
          int offset = invocation.getArgument(1);
          int length = invocation.getArgument(2);
          return input.read(buffer, offset, length);
        });
  }

  /**
   * Setups details for HTTP GET request.
   * @param toExcludeList SST file names to be excluded.
   */
  private void setupGetMethod(Collection<String> toExcludeList) {
    when(requestMock.getMethod()).thenReturn("GET");
    when(requestMock
        .getParameterValues(OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST))
        .thenReturn(toExcludeList.toArray(new String[0]));
  }

  private void prepSnapshotData() throws Exception {
    metaDir = OMStorage.getOmDbDir(conf);

    OzoneBucket bucket = TestDataUtil
        .createVolumeAndBucket(client);

    // Create dummy keys for snapshotting.
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(), ReplicationConfig
            .fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
        "content".getBytes(StandardCharsets.UTF_8));
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(), ReplicationConfig
            .fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
        "content".getBytes(StandardCharsets.UTF_8));

    snapshotDirName =
        createSnapshot(bucket.getVolumeName(), bucket.getName());
    snapshotDirName2 =
        createSnapshot(bucket.getVolumeName(), bucket.getName());

    // Create dummy snapshot to make sure it is not included.
    Path fabricatedSnapshot  = Paths.get(
        new File(snapshotDirName).getParent(),
        "fabricatedSnapshot");
    assertTrue(fabricatedSnapshot.toFile().mkdirs());
    assertTrue(Paths.get(fabricatedSnapshot.toString(),
        FABRICATED_FILE_NAME).toFile().createNewFile());

    // Create fabricated links to snapshot dirs
    // to confirm that links are recognized even if
    // they don't point to the checkpoint directory.
    Path fabricatedFile = Paths.get(snapshotDirName, FABRICATED_FILE_NAME);
    Path fabricatedLink = Paths.get(snapshotDirName2, FABRICATED_FILE_NAME);

    Files.write(fabricatedFile,
        "fabricatedData".getBytes(StandardCharsets.UTF_8));
    Files.createLink(fabricatedLink, fabricatedFile);

    // Simulate links from the compaction dir.
    compactionDirPath = Paths.get(metaDir.toString(),
        OM_SNAPSHOT_DIFF_DIR, DB_COMPACTION_SST_BACKUP_DIR);
    Path fabricatedLink2 = Paths.get(compactionDirPath.toString(),
        FABRICATED_FILE_NAME);
    Files.createLink(fabricatedLink2, fabricatedFile);
    Path currentFile = Paths.get(metaDir.toString(),
                                    OM_DB_NAME, "CURRENT");
    Path currentLink = Paths.get(compactionDirPath.toString(), "CURRENT");
    Files.createLink(currentLink, currentFile);

    dbCheckpoint = om.getMetadataManager()
        .getStore()
        .getCheckpoint(true);

  }

  private String createSnapshot(String vname, String bname)
      throws IOException, InterruptedException, TimeoutException {
    String snapshotName = UUID.randomUUID().toString();
    OzoneManagerProtocol writeClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();

    writeClient.createSnapshot(vname, bname, snapshotName);
    SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(vname, bname, snapshotName));
    String snapshotPath = getSnapshotPath(conf, snapshotInfo, 0)
        + OM_KEY_PREFIX;
    GenericTestUtils.waitFor(() -> new File(snapshotPath).exists(),
        100, 30000);
    return snapshotPath;
  }

  private Set<String> getFiles(Path path, int truncateLength)
      throws IOException {
    return getFiles(path, truncateLength, new HashSet<>());
  }

  // Get all files below path, recursively, (skipping fabricated files, archive directory in rocksdb).
  private Set<String> getFiles(Path path, int truncateLength,
      Set<String> fileSet) throws IOException {
    try (Stream<Path> files = Files.list(path)) {
      for (Path file : files.collect(Collectors.toList())) {
        if (file.toFile().isDirectory()) {
          getFiles(file, truncateLength, fileSet);
        }
        String filename = String.valueOf(file.getFileName());
        Path parentDir = file.getParent();
        String parentFileName = parentDir == null ? "null" : parentDir.toFile().getName();
        if (!filename.startsWith("fabricated") &&
            !filename.startsWith(OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME) &&
            !(filename.equals("archive") && parentFileName.startsWith("om.db"))) {
          fileSet.add(truncateFileName(truncateLength, file));
        }
      }
    }
    return fileSet;
  }

  /**
   * Confirm fabricated link lines in hardlink file are properly
   * formatted: "dir1/fabricatedFile dir2/fabricatedFile".
   *
   * The "fabricated" files/links are ones I've created by hand to
   * fully test the code, (as opposed to the "natural" files/links
   * created by the create snapshot process).
   *
   * @param directories Possible directories for the links to exist in.
   * @param lines Text lines defining the link paths.
   * @param testDirName Name of test directory.
   */
  private void checkFabricatedLines(Set<String> directories, List<String> lines,
                                    String testDirName) {
    // find the real file
    String realDir = null;
    for (String dir: directories) {
      if (Paths.get(testDirName, dir, FABRICATED_FILE_NAME).toFile().exists()) {
        assertNull(realDir, "Exactly one copy of the fabricated file exists in the tarball");
        realDir = dir;
      }
    }

    assertNotNull(realDir, "real directory found");
    directories.remove(realDir);
    Iterator<String> directoryIterator = directories.iterator();
    String dir0 = directoryIterator.next();
    String dir1 = directoryIterator.next();
    assertNotEquals("link directories are different", dir0, dir1);

    for (String line : lines) {
      String[] files = line.split("\t");
      assertTrue(
          files[0].startsWith(dir0) || files[0].startsWith(dir1),
          "fabricated entry contains valid first directory: " + line);
      assertTrue(files[1].startsWith(realDir),
          "fabricated entry contains correct real directory: " + line);
      Path path0 = Paths.get(files[0]);
      Path path1 = Paths.get(files[1]);
      assertEquals(FABRICATED_FILE_NAME,
          String.valueOf(path0.getFileName()),
          "fabricated entries contains correct file name: " + line);
      assertEquals(FABRICATED_FILE_NAME,
          String.valueOf(path1.getFileName()),
          "fabricated entries contains correct file name: " + line);
    }
  }

  // Validates line in hard link file. should look something like:
  // "dir1/x.sst x.sst".
  private void checkLine(String shortSnapshotLocation,
                            String shortSnapshotLocation2,
                            String line) {
    String[] files = line.split("\t");
    assertTrue(files[0].startsWith(shortSnapshotLocation) ||
        files[0].startsWith(shortSnapshotLocation2),
        "hl entry starts with valid snapshot dir: " + line);

    String file0 = files[0].substring(shortSnapshotLocation.length() + 1);
    String file1 = files[1];
    assertEquals(file0, file1, "hl filenames are the same");
  }

  private void testBootstrapLocking() throws Exception {
    // Get the bootstrap state handlers
    KeyManager keyManager = om.getKeyManager();
    BootstrapStateHandler keyDeletingService =
        keyManager.getDeletingService();
    BootstrapStateHandler snapshotDeletingService =
        keyManager.getSnapshotDeletingService();
    BootstrapStateHandler sstFilteringService =
        keyManager.getSnapshotSstFilteringService();
    BootstrapStateHandler differ = om.getMetadataManager()
        .getStore()
        .getRocksDBCheckpointDiffer();

    ExecutorService executorService = Executors.newCachedThreadPool();

    OMDBCheckpointServlet omDbCheckpointServlet = new OMDBCheckpointServlet();

    OMDBCheckpointServlet spyServlet = spy(omDbCheckpointServlet);
    ServletContext servletContext = mock(ServletContext.class);
    when(servletContext.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .thenReturn(om);
    doReturn(servletContext).when(spyServlet).getServletContext();

    spyServlet.init();

    // Confirm the other handlers are locked out when the bootstrap
    //  servlet takes the lock.
    try (AutoCloseable ignoredLock = spyServlet.getBootstrapStateLock().acquireWriteLock()) {
      confirmServletLocksOutOtherHandler(keyDeletingService, executorService);
      confirmServletLocksOutOtherHandler(snapshotDeletingService,
          executorService);
      confirmServletLocksOutOtherHandler(sstFilteringService, executorService);
      confirmServletLocksOutOtherHandler(differ, executorService);
    }
    // Confirm the servlet is locked out when any of the other
    //  handlers takes the lock.
    confirmOtherHandlerLocksOutServlet(keyDeletingService, spyServlet,
        executorService);
    confirmOtherHandlerLocksOutServlet(snapshotDeletingService, spyServlet,
        executorService);
    confirmOtherHandlerLocksOutServlet(sstFilteringService, spyServlet,
        executorService);
    confirmOtherHandlerLocksOutServlet(differ, spyServlet,
        executorService);

    // Confirm that servlet takes the lock when none of the other
    //  handlers have it.
    Future<Boolean> servletTest = checkLock(spyServlet, executorService);
    assertTrue(servletTest.get(10000, TimeUnit.MILLISECONDS));

    executorService.shutdownNow();

  }

  // Confirms handler can't take look the servlet already has.  Assumes
  // the servlet has already taken the lock.
  private void confirmServletLocksOutOtherHandler(BootstrapStateHandler handler,
      ExecutorService executorService) {
    Future<Boolean> test = checkLock(handler, executorService);
    // Handler should fail to take the lock because the servlet has taken it.
    assertThrows(TimeoutException.class,
         () -> test.get(500, TimeUnit.MILLISECONDS));
  }

  // Confirms Servlet can't take lock when handler has it.
  private void confirmOtherHandlerLocksOutServlet(BootstrapStateHandler handler,
      BootstrapStateHandler servlet, ExecutorService executorService)
      throws InterruptedException {
    try (UncheckedAutoCloseable ignoredLock = handler.getBootstrapStateLock().acquireWriteLock()) {
      Future<Boolean> test = checkLock(servlet, executorService);
      // Servlet should fail to lock when other handler has taken it.
      assertThrows(TimeoutException.class,
          () -> test.get(500, TimeUnit.MILLISECONDS));
    }
  }

  // Confirm lock is available by having handler take and release it.
  private Future<Boolean> checkLock(BootstrapStateHandler handler,
      ExecutorService executorService) {
    return executorService.submit(() -> {
      try {
        handler.getBootstrapStateLock().acquireWriteLock().close();
        return true;
      } catch (InterruptedException e) {
      }
      return false;
    });

  }
}
