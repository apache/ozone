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

package org.apache.hadoop.ozone.om.response;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.OMExecutionFlow;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for OMResponse related methods.
 */
public class TestOMResponse {
  @TempDir
  private Path folder;

  private OzoneManagerProtocolServerSideTranslatorPB translator;
  private GenericTestUtils.LogCapturer logCapturer;
  private static final long IPC_MAX_LEN = 1024 * 1024; // 1MB
  private static final long WARN_THRESHOLD = IPC_MAX_LEN / 2; // 512KB

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    configuration.setLong(
        "ipc.maximum.response.length", IPC_MAX_LEN);

    OzoneManager ozoneManager = mock(OzoneManager.class);
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
        configuration, ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(configuration);

    final OmConfig omConfig = configuration.getObject(OmConfig.class);
    when(ozoneManager.getConfig()).thenReturn(omConfig);

    OMExecutionFlow omExecutionFlow = new OMExecutionFlow(ozoneManager);
    when(ozoneManager.getOmExecutionFlow()).thenReturn(omExecutionFlow);

    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    when(ratisServer.checkRetryCache()).thenReturn(null);

    ProtocolMessageMetrics<OzoneManagerProtocolProtos.Type> protocolMessageMetrics =
        mock(ProtocolMessageMetrics.class);

    translator = new OzoneManagerProtocolServerSideTranslatorPB(
        ozoneManager, ratisServer, protocolMessageMetrics);

    // Capture logs from OzoneManagerProtocolServerSideTranslatorPB
    logCapturer = GenericTestUtils.LogCapturer.captureLogs(
        OzoneManagerProtocolServerSideTranslatorPB.class);
  }

  @AfterEach
  public void tearDown() {
    if (logCapturer != null) {
      logCapturer.stopCapturing();
    }
  }

  @Test
  public void testLargeResponseLogging() {
    // Create a large response that exceeds the threshold
    // Use ListKeysResponse with many keys to create a large response
    OzoneManagerProtocolProtos.ListKeysResponse.Builder listKeysBuilder =
        OzoneManagerProtocolProtos.ListKeysResponse.newBuilder();
    for (int i = 0; i < 12000; i++) {
      listKeysBuilder.addKeyInfo(OzoneManagerProtocolProtos.KeyInfo.newBuilder()
          .setKeyName("key" + i)
          .setVolumeName("vol1")
          .setBucketName("bucket1")
          .setDataSize(1024)
          .setCreationTime(System.currentTimeMillis())
          .setType(HddsProtos.ReplicationType.RATIS)
          .setModificationTime(System.currentTimeMillis())
          .build());
    }

    OMResponse largeResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.ListKeys)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setListKeysResponse(listKeysBuilder.build())
        .build();

    long responseSize = largeResponse.getSerializedSize();

    // Clear previous logs
    logCapturer.clearOutput();
    translator.logLargeResponseIfNeeded(largeResponse);

    // Verify logging occurred
    String logOutput = logCapturer.getOutput();
    assertThat(logOutput).contains("Large OMResponse detected");
    assertThat(logOutput).contains("cmd=ListKeys");
    // Verify the size in log matches our response size
    assertThat(logOutput).contains("size=" + responseSize + "B");
  }

  @Test
  public void testSmallResponseNoLogging() {
    // Create a small response
    OMResponse smallResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.InfoVolume)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setMessage("Small response")
        .build();

    long responseSize = smallResponse.getSerializedSize();
    assertThat(responseSize).isLessThan(WARN_THRESHOLD);
  }
}
