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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.commons.io.FileUtils;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
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

import static org.apache.hadoop.ozone.om.OMDBCheckpointServlet.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.OMDBCheckpointServlet.fixFileName;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
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
  private File metaDir;
  private String snapshotDirName;
  private String snapshotDirName2;
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
        .currentTimeMillis(), ".tar.gz");

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
        .writeDbDataToStream(any(), any());
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

    doNothing().when(responseMock).setContentType("application/x-tgz");
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
  public void testWriteDbDataToStream()
      throws Exception {
    prepArchiveData();

    try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
        omDbCheckpointServletMock.writeDbDataToStream(dbCheckpoint,
            fileOutputStream);
    }

    // Untar the file into a temp folder to be examined
    String testDirName = folder.newFolder().getAbsolutePath();
    int testDirLength = testDirName.length() + 1;
    FileUtil.unTar(tempFile, new File(testDirName));


    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    int metaDirLength = metaDir.toString().length() + 1;
    Path finalCheckpointLocation =
        Paths.get(testDirName);

    // Confirm the checkpoint directories match, (after remove extras)
    Set<String> initialCheckpointSet = getFiles(checkpointLocation,
        checkpointLocation.toString().length() + 1);
    Set<String> finalCheckpointSet = getFiles(finalCheckpointLocation,
        testDirLength);
    finalCheckpointSet.remove(OM_SNAPSHOT_DIR);

    Assert.assertTrue("hardlink file exists in checkpoint dir",
        finalCheckpointSet.contains(OM_HARDLINK_FILE));
    finalCheckpointSet.remove(OM_HARDLINK_FILE);
    Assert.assertEquals(initialCheckpointSet, finalCheckpointSet);

    String shortSnapshotLocation =
        fixFileName(metaDirLength, Paths.get(snapshotDirName));
    String shortSnapshotLocation2 =
        fixFileName(metaDirLength, Paths.get(snapshotDirName2));
    Path finalSnapshotLocation =
        Paths.get(testDirName, shortSnapshotLocation);

    Assert.assertTrue("CURRENT file exists",
        Paths.get(finalSnapshotLocation.toString(),
        "CURRENT").toFile().exists());

    Set<String> initialSnapshotSet =
        getFiles(Paths.get(snapshotDirName), metaDirLength);
    Set<String> finalSnapshotSet =
        getFiles(finalSnapshotLocation, testDirLength);
    Assert.assertTrue("snapshot manifest found",
        finalSnapshotSet.stream().anyMatch(s -> s.contains("MANIFEST")));

    // check each line in the hard link file
    Stream<String> lines = Files.lines(Paths.get(testDirName,
        OM_HARDLINK_FILE));
    boolean dummyLinkFound = false;
    for (String line: lines.collect(Collectors.toList())) {
      Assert.assertFalse("CURRENT file is not a hard link",
          line.contains("CURRENT"));
      if (line.contains("dummyFile")) {
        dummyLinkFound = true;
        checkDummyFile(shortSnapshotLocation, shortSnapshotLocation2, line,
            testDirName);
      } else {
        checkLine(shortSnapshotLocation, shortSnapshotLocation2, line);
        if (line.startsWith(shortSnapshotLocation)) {
          finalSnapshotSet.add(line.split("\t")[0]);
        }
      }
    }
    Assert.assertTrue("dummy link found", dummyLinkFound);
    Assert.assertEquals("found expected snapshot files",
        initialSnapshotSet, finalSnapshotSet);
  }

  private Set<String> getFiles(Path path, int truncateLength)
      throws IOException {
    Set<String> fileSet = new HashSet<>();
    try (Stream<Path> files = Files.list(path)) {
      for (Path file : files.collect(Collectors.toList())) {
        if (file != null) {
          if (!file.getFileName().toString().equals("dummyFile")) {
            fileSet.add(fixFileName(truncateLength, file));
          }
        }
      }
    }
    return fileSet;
  }

  // tests to see that dummy entry in hardlink file looks something like:
  //  "dir1/dummyFile  dir2/dummyFile"
  private void checkDummyFile(String dir0, String dir1, String line,
                              String testDirName) {
    String[] files = line.split("\t");
    Assert.assertTrue("dummy entry contains first directory",
        files[0].startsWith(dir0) || files[1].startsWith(dir0));
    Assert.assertTrue("dummy entry contains second directory",
        files[0].startsWith(dir1) || files[1].startsWith(dir1));
    Path path0 = Paths.get(files[0]);
    Path path1 = Paths.get(files[1]);
    Assert.assertNotEquals("dummy entry parent directories differ",
        path0.getParent(), path1.getParent());
    Assert.assertTrue("dummy entries contains dummyFile name",
        path0.getFileName().toString().equals("dummyFile") &&
        path1.getFileName().toString().equals("dummyFile"));
    int dummyFileCount = 0;
    if (Paths.get(testDirName, dir0, "dummyFile").toFile().exists()) {
      dummyFileCount++;
    }
    if (Paths.get(testDirName, dir1, "dummyFile").toFile().exists()) {
      dummyFileCount++;
    }
    Assert.assertEquals("exactly one dummy file exists in the output dir",
        dummyFileCount, 1);
  }

  // tests line in hard link file looks something like:
  //  "dir1/x.sst x.sst"
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

  private String createSnapshot(String vname, String bname)
      throws IOException, InterruptedException, TimeoutException {
    final OzoneManager om = cluster.getOzoneManager();
    String snapshotName = UUID.randomUUID().toString();
    OzoneManagerProtocol writeClient = cluster.getRpcClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();

    writeClient.createSnapshot(vname, bname, snapshotName);
    SnapshotInfo snapshotInfo = om
        .getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(vname, bname, snapshotName));
    String snapshotPath = getSnapshotPath(conf, snapshotInfo)
        + OM_KEY_PREFIX;
    GenericTestUtils.waitFor(() -> new File(snapshotPath).exists(),
        100, 2000);
    return snapshotDirName;
  }

  private void prepArchiveData() throws Exception {

    setupCluster();
    metaDir = OMStorage.getOmDbDir(conf);

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(),
        "content");
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(),
        "content");

    // this sleep can be removed after this is fixed:
    //  https://issues.apache.org/jira/browse/HDDS-7279
    Thread.sleep(2000);
    snapshotDirName =
        createSnapshot(bucket.getVolumeName(), bucket.getName());
    snapshotDirName2 =
        createSnapshot(bucket.getVolumeName(), bucket.getName());


    // create dummy link from one snapshot dir to the other
    //  to confirm that links are recognized even if
    //  they are not in the checkpoint directory
    Path dummyFile = Paths.get(snapshotDirName, "dummyFile");
    Path dummyLink = Paths.get(snapshotDirName2, "dummyFile");
    Files.write(dummyFile, "dummyData".getBytes(StandardCharsets.UTF_8));
    Files.createLink(dummyLink, dummyFile);

    dbCheckpoint = cluster.getOzoneManager()
        .getMetadataManager().getStore()
        .getCheckpoint(true);
  }
}
