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

package org.apache.hadoop.ozone.om.ratis_snapshot;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.MULTIPART_FORM_DATA_BOUNDARY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link OmRatisSnapshotProvider}.
 */
public class TestOmRatisSnapshotProvider {

  private OmRatisSnapshotProvider omRatisSnapshotProvider;
  private URLConnectionFactory connectionFactory;
  private OMNodeDetails leader;
  private String leaderNodeId;
  private static final String CR_NL = "\r\n";
  public static final String CONTENT_DISPOSITION =
      "Content-Disposition: form-data; name=\""
          + OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST + "[]\""
          + CR_NL + CR_NL;
  private File targetFile;

  @BeforeEach
  public void setup(@TempDir File snapshotDir,
      @TempDir File downloadDir) throws IOException {
    targetFile = new File(downloadDir, "newfile");

    Map<String, OMNodeDetails> peerNodesMap = new HashMap<>();
    leaderNodeId = "1";
    leader = mock(OMNodeDetails.class);
    peerNodesMap.put(leaderNodeId, leader);

    HttpConfig.Policy httpPolicy = mock(HttpConfig.Policy.class);
    connectionFactory = mock(URLConnectionFactory.class);

    omRatisSnapshotProvider =
        new OmRatisSnapshotProvider(snapshotDir, peerNodesMap, httpPolicy,
            false, connectionFactory);
  }

  @Test
  public void testIsDiskFullOrQuotaIOExceptionDetectsNoSpaceMessage() {
    assertTrue(OmRatisSnapshotProvider.isDiskFullOrQuotaIOException(
        new IOException("No space left on device")));
  }

  @Test
  public void testIsDiskFullOrQuotaIOExceptionDetectsFileSystemExceptionReason() {
    IOException wrapped = new IOException("write failed",
        new FileSystemException("p", null, "No space left on device"));
    assertTrue(OmRatisSnapshotProvider.isDiskFullOrQuotaIOException(wrapped));
  }

  @Test
  public void testIsDiskFullOrQuotaIOExceptionReturnsFalseForOtherErrors() {
    assertFalse(OmRatisSnapshotProvider.isDiskFullOrQuotaIOException(
        new IOException("Connection reset")));
  }

  @Test
  public void testBootstrapDiskSpaceCheckSkippedWhenZero(@TempDir File snapshotDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_BOOTSTRAP_MIN_SPACE_KEY, "0GB");
    OmRatisSnapshotProvider provider =
        new OmRatisSnapshotProvider(conf, snapshotDir, new HashMap<>());
    assertDoesNotThrow(() -> provider.ensureBootstrapDiskSpace());
  }

  @Test
  public void testBootstrapDiskSpaceCheckFailsWhenBelowMinimum(@TempDir File snapshotDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_BOOTSTRAP_MIN_SPACE_KEY, "1024EB");
    OmRatisSnapshotProvider provider =
        new OmRatisSnapshotProvider(conf, snapshotDir, new HashMap<>());
    IOException ex =
        assertThrows(IOException.class, provider::ensureBootstrapDiskSpace);
    assertEquals(true,
        ex.getMessage().contains(OMConfigKeys.OZONE_OM_BOOTSTRAP_MIN_SPACE_KEY));
  }

  @Test
  public void testDownloadSnapshot() throws IOException,
      AuthenticationException {
    URL omCheckpointUrl = mock(URL.class);
    StringBuilder sb = getStringBuilder();
    when(leader.getOMDBCheckpointEndpointUrl(anyBoolean(),
        anyBoolean(), anyBoolean())).thenReturn(omCheckpointUrl);

    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connectionFactory.openConnection(any(URL.class), anyBoolean()))
        .thenReturn(connection);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    when(connection.getOutputStream()).thenReturn(outputStream);
    when(connection.getResponseCode()).thenReturn(HTTP_OK);
    InputStream inputStream =
        new ByteArrayInputStream(outputStream.toByteArray());
    when(connection.getInputStream()).thenReturn(inputStream);

    omRatisSnapshotProvider.downloadSnapshot(leaderNodeId, targetFile);

    sb.append("--").append(MULTIPART_FORM_DATA_BOUNDARY).append("--").append(CR_NL);
    assertEquals(sb.toString(),
        new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
  }

  @Test
  public void testWriteFormDataWithSstFile() throws IOException {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    List<String> sstFiles = new ArrayList<>();
    String fileName = "file1.sst";
    sstFiles.add(fileName);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StringBuilder sb = getStringBuilder();
    when(connection.getOutputStream()).thenReturn(outputStream);


    OmRatisSnapshotProvider.writeFormData(connection, sstFiles);

    sb.append(fileName).append(CR_NL)
        .append("--").append(MULTIPART_FORM_DATA_BOUNDARY).append("--").append(CR_NL);
    assertEquals(sb.toString(),
        new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
  }

  @Test
  public void testWriteFormDataWithoutSstFile() throws IOException {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StringBuilder sb = getStringBuilder();
    when(connection.getOutputStream()).thenReturn(outputStream);

    OmRatisSnapshotProvider.writeFormData(connection, new ArrayList<>());

    sb.append("--").append(MULTIPART_FORM_DATA_BOUNDARY).append("--").append(CR_NL);
    assertEquals(sb.toString(),
        new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
  }

  private static StringBuilder getStringBuilder() {
    StringBuilder sb = new StringBuilder();
    sb.append("--").append(MULTIPART_FORM_DATA_BOUNDARY).append(CR_NL)
        .append(CONTENT_DISPOSITION);
    return sb;
  }

}
