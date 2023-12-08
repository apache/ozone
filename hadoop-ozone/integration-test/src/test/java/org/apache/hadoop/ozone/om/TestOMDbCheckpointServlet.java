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

package org.apache.hadoop.ozone.om;

import javax.servlet.ServletContext;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.commons.io.FileUtils;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.MULTIPART_FORM_DATA_BOUNDARY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;

import org.apache.ozone.test.GenericTestUtils;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_PREFIX;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_SUFFIX;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.truncateFileName;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COMPACTION_LOG_FILE_NAME_SUFFIX;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Class used for testing the OM DB Checkpoint provider servlet.
 */
@Timeout(240)
public class TestOMDbCheckpointServlet {
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
  private OzoneConfiguration conf;
  private File tempFile;
  private ServletOutputStream servletOutputStream;
  private MiniOzoneCluster cluster = null;
  private OMMetrics omMetrics = null;
  private HttpServletRequest requestMock = null;
  private HttpServletResponse responseMock = null;
  private OMDBCheckpointServlet omDbCheckpointServletMock = null;
  private File metaDir;
  private String snapshotDirName;
  private String snapshotDirName2;
  private Path compactionDirPath;
  private DBCheckpoint dbCheckpoint;
  private String method;
  private File folder;
  private static final String FABRICATED_FILE_NAME = "fabricatedFile.sst";
  private FileOutputStream fileOutputStream;
  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws Exception
   */
  @BeforeEach
  public void init(@TempDir File tempDir) throws Exception {
    folder = tempDir;
    conf = new OzoneConfiguration();

    tempFile = File.createTempFile("temp_" + System
        .currentTimeMillis(), ".tar");

    fileOutputStream = new FileOutputStream(tempFile);

    servletOutputStream = new ServletOutputStream() {
      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void setWriteListener(WriteListener writeListener) {
      }

      @Override
      public void write(int b) throws IOException {
        fileOutputStream.write(b);
      }
    };
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtils.deleteQuietly(tempFile);
  }

  private void setupCluster() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    omMetrics = cluster.getOzoneManager().getMetrics();

    omDbCheckpointServletMock =
        mock(OMDBCheckpointServlet.class);

    BootstrapStateHandler.Lock lock =
        new OMDBCheckpointServlet.Lock(cluster.getOzoneManager());
    doCallRealMethod().when(omDbCheckpointServletMock).init();
    Assertions.assertNull(
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
        .thenReturn(cluster.getOzoneManager());
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH))
        .thenReturn("true");

    doCallRealMethod().when(omDbCheckpointServletMock).doGet(requestMock,
        responseMock);
    doCallRealMethod().when(omDbCheckpointServletMock).doPost(requestMock,
        responseMock);

    doCallRealMethod().when(omDbCheckpointServletMock)
        .writeDbDataToStream(any(), any(), any(), any(), any(), any());

    when(omDbCheckpointServletMock.getBootstrapStateLock())
        .thenReturn(lock);

    doCallRealMethod().when(omDbCheckpointServletMock).getCheckpoint(any(),
        anyBoolean());
  }

  @ParameterizedTest
  @MethodSource("getHttpMethods")
  public void testEndpoint(String httpMethod) throws Exception {
    this.method = httpMethod;

    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);

    setupCluster();

    final OzoneManager om = cluster.getOzoneManager();
    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        om.getOmAdminUsernames(),
        om.getOmAdminGroups(),
        om.isSpnegoEnabled());

    doNothing().when(responseMock).setContentType("application/x-tar");
    doNothing().when(responseMock).setHeader(anyString(), anyString());

    List<String> toExcludeList = new ArrayList<>();
    toExcludeList.add("sstFile1.sst");
    toExcludeList.add("sstFile2.sst");

    setupHttpMethod(toExcludeList);

    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.init();
    long initialCheckpointCount =
        omMetrics.getDBCheckpointMetrics().getNumCheckpoints();

    doEndpoint();

    Assertions.assertTrue(tempFile.length() > 0);
    Assertions.assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointCreationTimeTaken() > 0);
    Assertions.assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointStreamingTimeTaken() > 0);
    Assertions.assertTrue(omMetrics.getDBCheckpointMetrics().
        getNumCheckpoints() > initialCheckpointCount);

    Mockito.verify(omDbCheckpointServletMock).writeDbDataToStream(any(),
        any(), any(), eq(toExcludeList), any(), any());
  }

  @ParameterizedTest
  @MethodSource("getHttpMethods")
  public void testEndpointNotRatis(String httpMethod) throws Exception {
    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    testEndpoint(httpMethod);
  }

  @Test
  public void testDoPostWithInvalidContentType() throws Exception {
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);

    setupCluster();

    final OzoneManager om = cluster.getOzoneManager();

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

    Mockito.verify(responseMock).setStatus(HttpServletResponse.SC_BAD_REQUEST);
  }

  @ParameterizedTest
  @MethodSource("getHttpMethods")
  public void testSpnegoEnabled(String httpMethod) throws Exception {
    this.method = httpMethod;

    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, "");
    conf.set(OZONE_OM_HTTP_AUTH_TYPE, "kerberos");
    conf.set(OZONE_RECON_KERBEROS_PRINCIPAL_KEY, "recon/host1@REALM");

    setupCluster();

    final OzoneManager om = cluster.getOzoneManager();
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

    setupHttpMethod(new ArrayList<>());

    doEndpoint();

    // Response status should be set to 403 Forbidden since there was no user
    // principal set in the request
    verify(responseMock, times(1)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Set the principal to DN in request
    // This should also get denied since only OM and recon
    // users should be granted access to the servlet
    Principal userPrincipalMock = mock(Principal.class);
    when(userPrincipalMock.getName()).thenReturn("dn/localhost@REALM");
    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);

    doEndpoint();

    // Verify that the Response status is set to 403 again for DN user.
    verify(responseMock, times(2)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Now, set the principal to recon in request
    when(userPrincipalMock.getName()).thenReturn("recon/localhost@REALM");

    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    doEndpoint();

    // Recon user should be able to access the servlet and download the
    // snapshot
    Assertions.assertTrue(tempFile.length() > 0);
  }

  @Test
  public void testWriteDbDataToStream() throws Exception {
    prepSnapshotData();
    // Set http param to include snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn("true");

    // Create a "spy" dbstore keep track of the checkpoint.
    OzoneManager om = cluster.getOzoneManager();
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
    Path unExpectedLog = Paths.get(compactionLogDir, "unexpected" +
        COMPACTION_LOG_FILE_NAME_SUFFIX);
    String unExpectedLogStr = truncateFileName(metaDirLength, unExpectedLog);
    Path expectedSst = Paths.get(sstBackupDir, "expected.sst");
    String expectedSstStr = truncateFileName(metaDirLength, expectedSst);
    Path unExpectedSst = Paths.get(sstBackupDir, "unexpected.sst");
    String unExpectedSstStr = truncateFileName(metaDirLength, unExpectedSst);

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

      // put "unexpected" fabricated files onto the fs after the files
      // get copied to the temp dir.  Since these appear in the "real"
      // dir after the copy, they shouldn't exist in the final file
      // set.  That will show that the copy only happened from the temp dir.
      Files.write(unExpectedLog,
          "fabricatedData".getBytes(StandardCharsets.UTF_8));
      Files.write(unExpectedSst,
          "fabricatedData".getBytes(StandardCharsets.UTF_8));
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
    Assertions.assertEquals(tmpHardLinkFileCount, tmpHardLinkFileCount());

    // Verify that tarball request count reaches to zero once doGet completes.
    Assertions.assertEquals(0,
        dbStore.getRocksDBCheckpointDiffer().getTarballRequestCount());
    dbCheckpoint = realCheckpoint.get();

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.getAbsolutePath();
    int testDirLength = testDirName.length() + 1;
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    int newDbDirLength = newDbDirName.length() + 1;
    File newDbDir = new File(newDbDirName);
    Assertions.assertTrue(newDbDir.mkdirs());
    FileUtil.unTar(tempFile, newDbDir);

    // Move snapshot dir to correct location.
    Assertions.assertTrue(new File(newDbDirName, OM_SNAPSHOT_DIR)
        .renameTo(new File(newDbDir.getParent(), OM_SNAPSHOT_DIR)));

    // Confirm the checkpoint directories match, (after remove extras).
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Path finalCheckpointLocation = Paths.get(newDbDirName);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        newDbDirLength);

    Assertions.assertTrue(finalCheckpointSet.contains(OM_HARDLINK_FILE),
        "hardlink file exists in checkpoint dir");
    finalCheckpointSet.remove(OM_HARDLINK_FILE);
    Assertions.assertEquals(initialCheckpointSet, finalCheckpointSet);

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
        Assertions.assertFalse(line.contains("CURRENT"),
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
    Assertions.assertTrue(finalFullSet.contains(expectedLogStr));
    Assertions.assertTrue(finalFullSet.contains(expectedSstStr));
    Assertions.assertTrue(initialFullSet.contains(unExpectedLogStr));
    Assertions.assertTrue(initialFullSet.contains(unExpectedSstStr));

    // Remove the dummy files that should not have been copied over
    // from the expected data.
    initialFullSet.remove(unExpectedLogStr);
    initialFullSet.remove(unExpectedSstStr);
    Assertions.assertEquals(initialFullSet, finalFullSet,
        "expected snapshot files not found");
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

  @Test
  public void testWriteDbDataWithoutOmSnapshot()
      throws Exception {
    prepSnapshotData();

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        any(), any(), anyBoolean(), any(), any(), anyBoolean());
    omDbCheckpointServletMock.init();

    // Set http param to exclude snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn(null);

    // Get the tarball.
    Path tmpdir = Files.createTempDirectory("bootstrapData");
    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
      omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint, requestMock,
          fileOutputStream, new ArrayList<>(), new ArrayList<>(), tmpdir);
    }

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.getAbsolutePath();
    int testDirLength = testDirName.length() + 1;
    FileUtil.unTar(tempFile, new File(testDirName));

    // Confirm the checkpoint directories match.
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Path finalCheckpointLocation = Paths.get(testDirName);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        testDirLength);

    Assertions.assertEquals(initialCheckpointSet, finalCheckpointSet);
  }

  @Test
  public void testWriteDbDataWithToExcludeFileList()
      throws Exception {
    prepSnapshotData();

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        any(), any(), anyBoolean(), any(), any(), anyBoolean());
    omDbCheckpointServletMock.init();

    File dummyFile = new File(dbCheckpoint.getCheckpointLocation().toString(),
        "dummy.sst");
    try (OutputStreamWriter writer = new OutputStreamWriter(
        new FileOutputStream(dummyFile), StandardCharsets.UTF_8)) {
      writer.write("Dummy data.");
    }
    Assertions.assertTrue(dummyFile.exists());
    List<String> toExcludeList = new ArrayList<>();
    List<String> excludedList = new ArrayList<>();
    toExcludeList.add(dummyFile.getName());

    // Set http param to exclude snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn(null);

    // Get the tarball.
    Path tmpdir = Files.createTempDirectory("bootstrapData");
    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
      omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint, requestMock,
          fileOutputStream, toExcludeList, excludedList, tmpdir);
    }

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.getAbsolutePath();
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
    Assertions.assertTrue(initialCheckpointSet.contains(dummyFile.getName()));
  }

  /**
   * Calls endpoint in regards to parametrized HTTP method.
   */
  private void doEndpoint() {
    if (method.equals("POST")) {
      omDbCheckpointServletMock.doPost(requestMock, responseMock);
    } else {
      omDbCheckpointServletMock.doGet(requestMock, responseMock);
    }
  }

  /**
   * Parametrizes test with HTTP method.
   * @return HTTP method.
   */
  private static Stream<Arguments> getHttpMethods() {
    return Stream.of(arguments("POST"), arguments("GET"));
  }

  /**
   * Setups HTTP method details depending on parametrized HTTP method.
   * @param toExcludeList SST file names to be excluded.
   * @throws IOException
   */
  private void setupHttpMethod(List<String> toExcludeList) throws IOException {
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
  private void setupPostMethod(List<String> toExcludeList)
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
    ServletInputStream inputStream = Mockito.mock(ServletInputStream.class);
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
  private void setupGetMethod(List<String> toExcludeList) {
    when(requestMock.getMethod()).thenReturn("GET");
    when(requestMock
        .getParameterValues(OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST))
        .thenReturn(toExcludeList.toArray(new String[0]));
  }

  private void prepSnapshotData() throws Exception {
    setupCluster();
    metaDir = OMStorage.getOmDbDir(conf);

    OzoneBucket bucket = TestDataUtil
        .createVolumeAndBucket(cluster.newClient());

    // Create dummy keys for snapshotting.
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(),
        "content");
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(),
        "content");

    snapshotDirName =
        createSnapshot(bucket.getVolumeName(), bucket.getName());
    snapshotDirName2 =
        createSnapshot(bucket.getVolumeName(), bucket.getName());

    // Create dummy snapshot to make sure it is not included.
    Path fabricatedSnapshot  = Paths.get(
        new File(snapshotDirName).getParent(),
        "fabricatedSnapshot");
    fabricatedSnapshot.toFile().mkdirs();
    Assertions.assertTrue(Paths.get(fabricatedSnapshot.toString(),
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

    dbCheckpoint = cluster.getOzoneManager()
        .getMetadataManager().getStore()
        .getCheckpoint(true);

  }

  private String createSnapshot(String vname, String bname)
      throws IOException, InterruptedException, TimeoutException {
    final OzoneManager om = cluster.getOzoneManager();
    String snapshotName = UUID.randomUUID().toString();
    OzoneManagerProtocol writeClient = cluster.newClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();

    writeClient.createSnapshot(vname, bname, snapshotName);
    SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(vname, bname, snapshotName));
    String snapshotPath = getSnapshotPath(conf, snapshotInfo)
        + OM_KEY_PREFIX;
    GenericTestUtils.waitFor(() -> new File(snapshotPath).exists(),
        100, 2000);
    return snapshotPath;
  }

  private Set<String> getFiles(Path path, int truncateLength)
      throws IOException {
    return getFiles(path, truncateLength, new HashSet<>());
  }

  // Get all files below path, recursively, (skipping fabricated files).
  private Set<String> getFiles(Path path, int truncateLength,
      Set<String> fileSet) throws IOException {
    try (Stream<Path> files = Files.list(path)) {
      for (Path file : files.collect(Collectors.toList())) {
        if (file.toFile().isDirectory()) {
          getFiles(file, truncateLength, fileSet);
        }
        String filename = String.valueOf(file.getFileName());
        if (!filename.startsWith("fabricated") &&
            !filename.startsWith(OZONE_RATIS_SNAPSHOT_COMPLETE_FLAG_NAME)) {
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
        Assertions.assertNull(realDir,
            "Exactly one copy of the fabricated file exists in the tarball");
        realDir = dir;
      }
    }

    Assertions.assertNotNull(realDir, "real directory found");
    directories.remove(realDir);
    Iterator<String> directoryIterator = directories.iterator();
    String dir0 = directoryIterator.next();
    String dir1 = directoryIterator.next();
    Assertions.assertNotEquals("link directories are different", dir0, dir1);

    for (String line : lines) {
      String[] files = line.split("\t");
      Assertions.assertTrue(
          files[0].startsWith(dir0) || files[0].startsWith(dir1),
          "fabricated entry contains valid first directory: " + line);
      Assertions.assertTrue(files[1].startsWith(realDir),
          "fabricated entry contains correct real directory: " + line);
      Path path0 = Paths.get(files[0]);
      Path path1 = Paths.get(files[1]);
      Assertions.assertEquals(FABRICATED_FILE_NAME,
          String.valueOf(path0.getFileName()),
          "fabricated entries contains correct file name: " + line);
      Assertions.assertEquals(FABRICATED_FILE_NAME,
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
    Assertions.assertTrue(files[0].startsWith(shortSnapshotLocation) ||
        files[0].startsWith(shortSnapshotLocation2),
        "hl entry starts with valid snapshot dir: " + line);

    String file0 = files[0].substring(shortSnapshotLocation.length() + 1);
    String file1 = files[1];
    Assertions.assertEquals(file0, file1, "hl filenames are the same");
  }

  @Test
  public void testBootstrapLocking() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();

    // Get the bootstrap state handlers
    KeyManager keyManager = cluster.getOzoneManager().getKeyManager();
    BootstrapStateHandler keyDeletingService =
        keyManager.getDeletingService();
    BootstrapStateHandler snapshotDeletingService =
        keyManager.getSnapshotDeletingService();
    BootstrapStateHandler sstFilteringService =
        keyManager.getSnapshotSstFilteringService();
    BootstrapStateHandler differ =
        cluster.getOzoneManager().getMetadataManager()
            .getStore().getRocksDBCheckpointDiffer();

    ExecutorService executorService = Executors.newCachedThreadPool();

    OMDBCheckpointServlet omDbCheckpointServlet = new OMDBCheckpointServlet();

    OMDBCheckpointServlet spyServlet = spy(omDbCheckpointServlet);
    ServletContext servletContext = mock(ServletContext.class);
    when(servletContext.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .thenReturn(cluster.getOzoneManager());
    doReturn(servletContext).when(spyServlet).getServletContext();

    spyServlet.init();

    // Confirm the other handlers are locked out when the bootstrap
    //  servlet takes the lock.
    try (BootstrapStateHandler.Lock ignoredLock =
        spyServlet.getBootstrapStateLock().lock()) {
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
    Assertions.assertTrue(servletTest.get(10000, TimeUnit.MILLISECONDS));

    executorService.shutdownNow();

  }

  // Confirms handler can't take look the servlet already has.  Assumes
  // the servlet has already taken the lock.
  private void confirmServletLocksOutOtherHandler(BootstrapStateHandler handler,
      ExecutorService executorService) {
    Future<Boolean> test = checkLock(handler, executorService);
    // Handler should fail to take the lock because the servlet has taken it.
    Assert.assertThrows(TimeoutException.class,
         () -> test.get(500, TimeUnit.MILLISECONDS));
  }

  // Confirms Servlet can't take lock when handler has it.
  private void confirmOtherHandlerLocksOutServlet(BootstrapStateHandler handler,
      BootstrapStateHandler servlet, ExecutorService executorService)
      throws InterruptedException {
    try (BootstrapStateHandler.Lock ignoredLock =
        handler.getBootstrapStateLock().lock()) {
      Future<Boolean> test = checkLock(servlet, executorService);
      // Servlet should fail to lock when other handler has taken it.
      Assert.assertThrows(TimeoutException.class,
          () -> test.get(500, TimeUnit.MILLISECONDS));
    }
  }

  // Confirm lock is available by having handler take and release it.
  private Future<Boolean> checkLock(BootstrapStateHandler handler,
      ExecutorService executorService) {
    return executorService.submit(() -> {
      try {
        handler.getBootstrapStateLock().lock();
        handler.getBootstrapStateLock().unlock();
        return true;
      } catch (InterruptedException e) {
      }
      return false;
    });

  }
}
