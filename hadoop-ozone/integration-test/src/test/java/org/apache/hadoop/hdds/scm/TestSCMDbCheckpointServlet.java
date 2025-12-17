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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.MULTIPART_FORM_DATA_BOUNDARY;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.server.SCMDBCheckpointServlet;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Class used for testing the SCM DB Checkpoint provider servlet.
 */
public class TestSCMDbCheckpointServlet {
  private MiniOzoneCluster cluster = null;
  private SCMMetrics scmMetrics;
  private HttpServletRequest requestMock;
  private HttpServletResponse responseMock;
  private String method;
  private SCMDBCheckpointServlet scmDbCheckpointServletMock;

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
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
        .writeDbDataToStream(any(), any(), any(), any(), any());
    doCallRealMethod().when(scmDbCheckpointServletMock).doPost(requestMock,
        responseMock);
    doCallRealMethod().when(scmDbCheckpointServletMock).doGet(requestMock,
        responseMock);
    doCallRealMethod().when(scmDbCheckpointServletMock).getCheckpoint(any(),
        anyBoolean());
    doCallRealMethod().when(scmDbCheckpointServletMock)
        .processMetadataSnapshotRequest(any(), any(), anyBoolean(), anyBoolean());

    ServletContext servletContextMock = mock(ServletContext.class);
    when(scmDbCheckpointServletMock.getServletContext())
        .thenReturn(servletContextMock);
    when(servletContextMock.getAttribute(OzoneConsts.SCM_CONTEXT_ATTRIBUTE))
        .thenReturn(cluster.getStorageContainerManager());
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("getHttpMethods")
  void testEndpoint(String httpMethod, @TempDir Path tempDir)
      throws ServletException, IOException, InterruptedException {
    this.method = httpMethod;

    Set<String> toExcludeList = new HashSet<>();
    toExcludeList.add("sstFile1.sst");
    toExcludeList.add("sstFile2.sst");

    setupHttpMethod(toExcludeList);

    doNothing().when(responseMock).setContentType("application/x-tgz");
    doNothing().when(responseMock).setHeader(anyString(), anyString());

    final Path outputPath = tempDir.resolve("testEndpoint.tar");
    when(responseMock.getOutputStream()).thenReturn(
        new ServletOutputStream() {
          private final OutputStream fileOutputStream = Files.newOutputStream(outputPath);

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
        });

    when(scmDbCheckpointServletMock.getBootstrapStateLock()).thenReturn(
        new DBCheckpointServlet.NoOpLock());
    scmDbCheckpointServletMock.init();
    long initialCheckpointCount =
        scmMetrics.getDBCheckpointMetrics().getNumCheckpoints();

    doEndpoint();

    assertThat(outputPath.toFile().length()).isGreaterThan(0);
    assertThat(scmMetrics.getDBCheckpointMetrics().getLastCheckpointCreationTimeTaken())
        .isGreaterThan(0);
    assertThat(scmMetrics.getDBCheckpointMetrics().getLastCheckpointStreamingTimeTaken())
        .isGreaterThan(0);
    assertThat(scmMetrics.getDBCheckpointMetrics().getNumCheckpoints())
        .isGreaterThan(initialCheckpointCount);

    verify(scmDbCheckpointServletMock).writeDbDataToStream(any(),
        any(), any(), eq(toExcludeList), any());
  }

  @Test
  public void testDoPostWithInvalidContentType() throws ServletException {
    when(requestMock.getContentType()).thenReturn("application/json");

    scmDbCheckpointServletMock.init();

    scmDbCheckpointServletMock.doPost(requestMock, responseMock);

    verify(responseMock).setStatus(HttpServletResponse.SC_BAD_REQUEST);
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
  private void setupHttpMethod(Collection<String> toExcludeList) throws IOException {
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

}
