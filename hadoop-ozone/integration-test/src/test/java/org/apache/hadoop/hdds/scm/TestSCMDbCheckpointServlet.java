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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.server.SCMDBCheckpointServlet;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.commons.io.FileUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.MULTIPART_FORM_DATA_BOUNDARY;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
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
  private HttpServletRequest requestMock;
  private HttpServletResponse responseMock;
  private String method;
  private SCMDBCheckpointServlet scmDbCheckpointServletMock;
  private ServletContext servletContextMock;

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

    requestMock = mock(HttpServletRequest.class);
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH))
        .thenReturn("true");

    responseMock = mock(HttpServletResponse.class);

    scmDbCheckpointServletMock = mock(SCMDBCheckpointServlet.class);
    doCallRealMethod().when(scmDbCheckpointServletMock).init();
    doCallRealMethod().when(scmDbCheckpointServletMock).initialize(
        scm.getScmMetadataStore().getStore(),
        scmMetrics.getDBCheckpointMetrics(),
        false,
        Collections.emptyList(),
        Collections.emptyList(),
        false);
    doCallRealMethod().when(scmDbCheckpointServletMock)
        .writeDbDataToStream(any(), any(), any(), any(), any(), any());
    doCallRealMethod().when(scmDbCheckpointServletMock).doPost(requestMock,
        responseMock);
    doCallRealMethod().when(scmDbCheckpointServletMock).doGet(requestMock,
        responseMock);
    doCallRealMethod().when(scmDbCheckpointServletMock).getCheckpoint(any(),
        anyBoolean());

    servletContextMock = mock(ServletContext.class);
    when(scmDbCheckpointServletMock.getServletContext())
        .thenReturn(servletContextMock);
    when(servletContextMock.getAttribute(OzoneConsts.SCM_CONTEXT_ATTRIBUTE))
        .thenReturn(cluster.getStorageContainerManager());
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

  @ParameterizedTest
  @MethodSource("getHttpMethods")
  public void testEndpoint(String httpMethod)
      throws ServletException, IOException, InterruptedException {
    this.method = httpMethod;

    File tempFile = null;
    try {
      List<String> toExcludeList = new ArrayList<>();
      toExcludeList.add("sstFile1.sst");
      toExcludeList.add("sstFile2.sst");

      setupHttpMethod(toExcludeList);

      doNothing().when(responseMock).setContentType("application/x-tgz");
      doNothing().when(responseMock).setHeader(Mockito.anyString(),
          Mockito.anyString());

      tempFile = File.createTempFile("testEndpoint_" + System
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

      when(scmDbCheckpointServletMock.getBootstrapStateLock()).thenReturn(
          new DBCheckpointServlet.Lock());
      scmDbCheckpointServletMock.init();
      long initialCheckpointCount =
          scmMetrics.getDBCheckpointMetrics().getNumCheckpoints();

      doEndpoint();

      Assertions.assertTrue(tempFile.length() > 0);
      Assertions.assertTrue(
          scmMetrics.getDBCheckpointMetrics().
              getLastCheckpointCreationTimeTaken() > 0);
      Assertions.assertTrue(
          scmMetrics.getDBCheckpointMetrics().
              getLastCheckpointStreamingTimeTaken() > 0);
      Assertions.assertTrue(scmMetrics.getDBCheckpointMetrics().
          getNumCheckpoints() > initialCheckpointCount);

      Mockito.verify(scmDbCheckpointServletMock).writeDbDataToStream(any(),
          any(), any(), eq(toExcludeList), any(), any());
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }

  }

  @Test
  public void testDoPostWithInvalidContentType() throws ServletException {
    when(requestMock.getContentType()).thenReturn("application/json");

    scmDbCheckpointServletMock.init();

    scmDbCheckpointServletMock.doPost(requestMock, responseMock);

    Mockito.verify(responseMock).setStatus(HttpServletResponse.SC_BAD_REQUEST);
  }

  /**
   * Calls endpoint in regards to parametrized HTTP method.
   */
  private void doEndpoint() {
    if (method.equals("POST")) {
      scmDbCheckpointServletMock.doPost(requestMock, responseMock);
    } else {
      scmDbCheckpointServletMock.doGet(requestMock, responseMock);
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

}
