/**
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
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.commons.io.FileUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.om.OMDBCheckpointServlet.writeOmDBCheckpointToStream;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Matchers;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Class used for testing the OM DB Checkpoint provider servlet.
 */
public class TestOMDbCheckpointServlet {
  private MiniOzoneCluster cluster = null;
  private OMMetrics omMetrics;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omId;

  @Rule
  public Timeout timeout = new Timeout(60000);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOmId(omId)
        .build();
    cluster.waitForClusterToBeReady();
    omMetrics = cluster.getOzoneManager().getMetrics();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDoGet() throws ServletException, IOException {

    File tempFile = null;
    try {
      OMDBCheckpointServlet omDbCheckpointServletMock =
          mock(OMDBCheckpointServlet.class);

      doCallRealMethod().when(omDbCheckpointServletMock).init();

      HttpServletRequest requestMock = mock(HttpServletRequest.class);
      // Return current user short name when asked
      when(requestMock.getRemoteUser())
          .thenReturn(UserGroupInformation.getCurrentUser().getShortUserName());
      HttpServletResponse responseMock = mock(HttpServletResponse.class);

      ServletContext servletContextMock = mock(ServletContext.class);
      when(omDbCheckpointServletMock.getServletContext())
          .thenReturn(servletContextMock);

      when(servletContextMock.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
          .thenReturn(cluster.getOzoneManager());
      when(requestMock.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH))
          .thenReturn("true");
      doNothing().when(responseMock).setContentType("application/x-tgz");
      doNothing().when(responseMock).setHeader(Matchers.anyString(),
          Matchers.anyString());

      tempFile = File.createTempFile("testDoGet_" + System
          .currentTimeMillis(), ".tar.gz");

      FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
      when(responseMock.getOutputStream()).thenReturn(
          new ServletOutputStream() {
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
          });

      doCallRealMethod().when(omDbCheckpointServletMock).doGet(requestMock,
          responseMock);

      omDbCheckpointServletMock.init();
      long initialCheckpointCount = omMetrics.getNumCheckpoints();

      omDbCheckpointServletMock.doGet(requestMock, responseMock);

      Assert.assertTrue(tempFile.length() > 0);
      Assert.assertTrue(
          omMetrics.getLastCheckpointCreationTimeTaken() > 0);
      Assert.assertTrue(
          omMetrics.getLastCheckpointStreamingTimeTaken() > 0);
      Assert.assertTrue(omMetrics.getNumCheckpoints() > initialCheckpointCount);
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }

  }

  @Test
  public void testWriteCheckpointToOutputStream() throws Exception {

    FileInputStream fis = null;
    FileOutputStream fos = null;

    try {
      String testDirName = folder.newFolder().getAbsolutePath();
      File file = new File(testDirName + "/temp1.txt");
      FileWriter writer = new FileWriter(file);
      writer.write("Test data 1");
      writer.close();

      file = new File(testDirName + "/temp2.txt");
      writer = new FileWriter(file);
      writer.write("Test data 2");
      writer.close();

      File outputFile =
          new File(Paths.get(testDirName, "output_file.tgz").toString());
      TestDBCheckpoint dbCheckpoint = new TestDBCheckpoint(
          Paths.get(testDirName));
      writeOmDBCheckpointToStream(dbCheckpoint,
          new FileOutputStream(outputFile));
      assertNotNull(outputFile);
    } finally {
      IOUtils.closeStream(fis);
      IOUtils.closeStream(fos);
    }
  }
}

class TestDBCheckpoint implements DBCheckpoint {

  private Path checkpointFile;

  TestDBCheckpoint(Path checkpointFile) {
    this.checkpointFile = checkpointFile;
  }

  @Override
  public Path getCheckpointLocation() {
    return checkpointFile;
  }

  @Override
  public long getCheckpointTimestamp() {
    return 0;
  }

  @Override
  public long getLatestSequenceNumber() {
    return 0;
  }

  @Override
  public long checkpointCreationTimeTaken() {
    return 0;
  }

  @Override
  public void cleanupCheckpoint() throws IOException {
    FileUtils.deleteDirectory(checkpointFile.toFile());
  }
}