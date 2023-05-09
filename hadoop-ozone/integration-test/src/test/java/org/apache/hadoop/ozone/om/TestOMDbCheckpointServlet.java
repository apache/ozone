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
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;


import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Matchers;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.truncateFileName;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.mockito.ArgumentMatchers.any;
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
public class TestOMDbCheckpointServlet {
  private OzoneConfiguration conf;
  private File tempFile;
  private ServletOutputStream servletOutputStream;
  private MiniOzoneCluster cluster = null;
  private OMMetrics omMetrics = null;
  private HttpServletRequest requestMock = null;
  private HttpServletResponse responseMock = null;
  private OMDBCheckpointServlet omDbCheckpointServletMock = null;
  private BootstrapStateHandler.Lock lock;
  private File metaDir;
  private String snapshotDirName;
  private String snapshotDirName2;
  private Path compactionDirPath;
  private DBCheckpoint dbCheckpoint;

  @Rule
  public Timeout timeout = Timeout.seconds(240);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws Exception
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();

    tempFile = File.createTempFile("testDoGet_" + System
        .currentTimeMillis(), ".tar");

    FileOutputStream fileOutputStream = new FileOutputStream(tempFile);

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
  @After
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

    lock = new OMDBCheckpointServlet.Lock(cluster.getOzoneManager());
    doCallRealMethod().when(omDbCheckpointServletMock).init();

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

    doCallRealMethod().when(omDbCheckpointServletMock)
        .writeDbDataToStream(any(), any(), any(), any(), any());

    when(omDbCheckpointServletMock.getBootstrapStateLock())
        .thenReturn(lock);
  }

  @Test
  public void testDoGet() throws Exception {
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
    doNothing().when(responseMock).setHeader(Matchers.anyString(),
        Matchers.anyString());

    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.init();
    long initialCheckpointCount =
        omMetrics.getDBCheckpointMetrics().getNumCheckpoints();

    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    Assert.assertTrue(tempFile.length() > 0);
    Assert.assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointCreationTimeTaken() > 0);
    Assert.assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointStreamingTimeTaken() > 0);
    Assert.assertTrue(omMetrics.getDBCheckpointMetrics().
        getNumCheckpoints() > initialCheckpointCount);
  }

  @Test
  public void testSpnegoEnabled() throws Exception {
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
    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    // Response status should be set to 403 Forbidden since there was no user
    // principal set in the request
    verify(responseMock, times(1)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Set the principal to DN in request
    // This should also get denied since only OM and recon
    // users should be granted access to the servlet
    Principal userPrincipalMock = mock(Principal.class);
    when(userPrincipalMock.getName()).thenReturn("dn/localhost@REALM");
    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);

    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    // Verify that the Response status is set to 403 again for DN user.
    verify(responseMock, times(2)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Now, set the principal to recon in request
    when(userPrincipalMock.getName()).thenReturn("recon/localhost@REALM");

    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    // Recon user should be able to access the servlet and download the
    // snapshot
    Assert.assertTrue(tempFile.length() > 0);
  }

  @Test
  public void testWriteDbDataToStream() throws Exception {
    prepSnapshotData();
    // Set http param to include snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn("true");

    // Get the tarball.
    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
      omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint, requestMock,
          fileOutputStream, new ArrayList<>(), new ArrayList<>());
    }

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.newFolder().getAbsolutePath();
    int testDirLength = testDirName.length() + 1;
    String newDbDirName = testDirName + OM_KEY_PREFIX + OM_DB_NAME;
    int newDbDirLength = newDbDirName.length() + 1;
    File newDbDir = new File(newDbDirName);
    newDbDir.mkdirs();
    FileUtil.unTar(tempFile, newDbDir);

    // Move snapshot dir to correct location.
    Assert.assertTrue(new File(newDbDirName, OM_SNAPSHOT_DIR)
        .renameTo(new File(newDbDir.getParent(), OM_SNAPSHOT_DIR)));

    // Confirm the checkpoint directories match, (after remove extras).
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Path finalCheckpointLocation = Paths.get(newDbDirName);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        newDbDirLength);

    Assert.assertTrue("hardlink file exists in checkpoint dir",
        finalCheckpointSet.contains(OM_HARDLINK_FILE));
    finalCheckpointSet.remove(OM_HARDLINK_FILE);
    Assert.assertEquals(initialCheckpointSet, finalCheckpointSet);

    int metaDirLength = metaDir.toString().length() + 1;
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
        Assert.assertFalse("CURRENT file is not a hard link",
            line.contains("CURRENT"));
        if (line.contains("fabricatedFile")) {
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
    Assert.assertEquals("expected snapshot files not found",
        initialFullSet, finalFullSet);
  }

  @Test
  public void testWriteDbDataWithoutOmSnapshot()
      throws Exception {
    prepSnapshotData();

    // Set http param to exclude snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn(null);

    // Get the tarball.
    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
      omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint, requestMock,
          fileOutputStream, new ArrayList<>(), new ArrayList<>());
    }

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.newFolder().getAbsolutePath();
    int testDirLength = testDirName.length() + 1;
    FileUtil.unTar(tempFile, new File(testDirName));

    // Confirm the checkpoint directories match.
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Path finalCheckpointLocation = Paths.get(testDirName);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        testDirLength);

    Assert.assertEquals(initialCheckpointSet, finalCheckpointSet);
  }

  @Test
  public void testWriteDbDataWithToExcludeFileList()
      throws Exception {
    prepSnapshotData();

    File dummyFile = new File(dbCheckpoint.getCheckpointLocation().toString(),
        "dummy.sst");
    try (OutputStreamWriter writer = new OutputStreamWriter(
        new FileOutputStream(dummyFile), StandardCharsets.UTF_8)) {
      writer.write("Dummy data.");
    }
    Assert.assertTrue(dummyFile.exists());
    List<String> toExcludeList = new ArrayList<>();
    List<String> excludedList = new ArrayList<>();
    toExcludeList.add(dummyFile.getName());

    // Set http param to exclude snapshot data.
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA))
        .thenReturn(null);

    // Get the tarball.
    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
      omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint, requestMock,
          fileOutputStream, toExcludeList, excludedList);
    }

    // Untar the file into a temp folder to be examined.
    String testDirName = folder.newFolder().getAbsolutePath();
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
    Assert.assertTrue(initialCheckpointSet.contains(dummyFile.getName()));
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
    Assert.assertTrue(Paths.get(fabricatedSnapshot.toString(), "fabricatedFile")
        .toFile().createNewFile());

    // Create fabricated links to snapshot dirs
    // to confirm that links are recognized even if
    // they are don't point to the checkpoint directory.
    Path fabricatedFile = Paths.get(snapshotDirName, "fabricatedFile");
    Path fabricatedLink = Paths.get(snapshotDirName2, "fabricatedFile");

    Files.write(fabricatedFile,
        "fabricatedData".getBytes(StandardCharsets.UTF_8));
    Files.createLink(fabricatedLink, fabricatedFile);

    // Simulate links from the compaction dir.
    compactionDirPath = Paths.get(metaDir.toString(),
        OM_SNAPSHOT_DIFF_DIR, DB_COMPACTION_SST_BACKUP_DIR);
    Path fabricatedLink2 = Paths.get(compactionDirPath.toString(),
        "fabricatedFile");
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
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  private Set<String> getFiles(Path path, int truncateLength,
      Set<String> fileSet) throws IOException {
    try (Stream<Path> files = Files.list(path)) {
      for (Path file : files.collect(Collectors.toList())) {
        if (file.toFile().isDirectory()) {
          getFiles(file, truncateLength, fileSet);
        }
        if (!file.getFileName().toString().startsWith("fabricated")) {
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
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  private void checkFabricatedLines(Set<String> directories, List<String> lines,
                                    String testDirName) {
    // find the real file
    String realDir = null;
    for (String dir: directories) {
      if (Paths.get(testDirName, dir, "fabricatedFile").toFile().exists()) {
        Assert.assertNull(
            "Exactly one copy of the fabricated file exists in the tarball",
            realDir);
        realDir = dir;
      }
    }

    Assert.assertNotNull("real directory found", realDir);
    directories.remove(realDir);
    Iterator<String> directoryIterator = directories.iterator();
    String dir0 = directoryIterator.next();
    String dir1 = directoryIterator.next();
    Assert.assertNotEquals("link directories are different", dir0, dir1);

    for (String line : lines) {
      String[] files = line.split("\t");
      Assert.assertTrue("fabricated entry contains valid first directory",
          files[0].startsWith(dir0) || files[0].startsWith(dir1));
      Assert.assertTrue("fabricated entry contains correct real directory",
          files[1].startsWith(realDir));
      Path path0 = Paths.get(files[0]);
      Path path1 = Paths.get(files[1]);
      Assert.assertTrue("fabricated entries contains correct file name",
          path0.getFileName().toString().equals("fabricatedFile") &&
              path1.getFileName().toString().equals("fabricatedFile"));
    }
  }

  // Validates line in hard link file. should look something like:
  // "dir1/x.sst x.sst".
  private void checkLine(String shortSnapshotLocation,
                            String shortSnapshotLocation2,
                            String line) {
    String[] files = line.split("\t");
    Assert.assertTrue("hl entry starts with valid snapshot dir",
        files[0].startsWith(shortSnapshotLocation) ||
        files[0].startsWith(shortSnapshotLocation2));

    String file0 = files[0].substring(shortSnapshotLocation.length() + 1);
    String file1 = files[1];
    Assert.assertEquals("hl filenames are the same", file0, file1);
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
    try (BootstrapStateHandler.Lock lock =
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
    Assert.assertTrue(servletTest.get(10000, TimeUnit.MILLISECONDS));

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
    try (BootstrapStateHandler.Lock lock =
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
