/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.server.SCMDBCheckpointServlet;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.commons.io.FileUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Matchers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Class used for testing the SCM DB Checkpoint provider servlet.
 */
@Timeout(240)
public class TestSCMDbCheckpointServlet {
  private MiniOzoneCluster cluster = null;
  private StorageContainerManager scm;
  private SCMMetrics scmMetrics;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omId;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws Exception
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOmId(omId)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    scmMetrics = StorageContainerManager.getMetrics();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDoGet()
      throws ServletException, IOException, CompressorException,
      InterruptedException {

    File tempFile = null;
    try {
      SCMDBCheckpointServlet scmDbCheckpointServletMock =
          mock(SCMDBCheckpointServlet.class);

      doCallRealMethod().when(scmDbCheckpointServletMock).init();
      doCallRealMethod().when(scmDbCheckpointServletMock).initialize(
          scm.getScmMetadataStore().getStore(),
          scmMetrics.getDBCheckpointMetrics(),
          false,
          Collections.emptyList(),
          Collections.emptyList(),
          false);
      doCallRealMethod().when(scmDbCheckpointServletMock)
         .writeDbDataToStream(any(), any(), any());

      HttpServletRequest requestMock = mock(HttpServletRequest.class);
      HttpServletResponse responseMock = mock(HttpServletResponse.class);

      ServletContext servletContextMock = mock(ServletContext.class);
      when(scmDbCheckpointServletMock.getServletContext())
          .thenReturn(servletContextMock);

      when(servletContextMock.getAttribute(OzoneConsts.SCM_CONTEXT_ATTRIBUTE))
          .thenReturn(cluster.getStorageContainerManager());
      when(requestMock.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH))
          .thenReturn("true");
      doNothing().when(responseMock).setContentType("application/x-tgz");
      doNothing().when(responseMock).setHeader(Matchers.anyString(),
          Matchers.anyString());

      tempFile = File.createTempFile("testDoGet_" + System
          .currentTimeMillis(), ".tar");

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

      doCallRealMethod().when(scmDbCheckpointServletMock).doGet(requestMock,
          responseMock);

      scmDbCheckpointServletMock.init();
      long initialCheckpointCount =
          scmMetrics.getDBCheckpointMetrics().getNumCheckpoints();

      scmDbCheckpointServletMock.doGet(requestMock, responseMock);

      Assertions.assertTrue(tempFile.length() > 0);
      Assertions.assertTrue(
          scmMetrics.getDBCheckpointMetrics().
              getLastCheckpointCreationTimeTaken() > 0);
      Assertions.assertTrue(
          scmMetrics.getDBCheckpointMetrics().
              getLastCheckpointStreamingTimeTaken() > 0);
      Assertions.assertTrue(scmMetrics.getDBCheckpointMetrics().
          getNumCheckpoints() > initialCheckpointCount);
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }

  }
}
