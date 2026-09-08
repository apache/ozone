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

package org.apache.hadoop.ozone.client.rpc.read;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientShortCircuit;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.hdds.scm.storage.LocalChunkInputStream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.om.BucketForTesting;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests {@link LocalChunkInputStream}.
 * For local intellij run, please follow the steps below:
 *  Add Environment variables
 *  LD_LIBRARY_PATH=$PROJECT_DIR$/target/native-lib
 *  DYLD_LIBRARY_PATH=$PROJECT_DIR$/target/native-lib
 *  to intellij run configuration.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestLocalChunkInputStream extends InputStreamTests {

  private LogCapturer shortCircuitClientLog;
  private LogCapturer grpcClientLog;

  @BeforeAll
  @Override
  void setup() throws Exception {
    assumeTrue(DomainSocketFactory.isNativeLibraryLoaded());
    super.setup();
    assumeTrue(DomainSocketFactory.getInstance(getCluster().getConf()).isServiceReady());
  }

  @BeforeEach
  void startCapturing() {
    shortCircuitClientLog = LogCapturer.captureLogs(XceiverClientShortCircuit.LOG);
    grpcClientLog = LogCapturer.captureLogs(XceiverClientGrpc.LOG);
  }

  @AfterEach
  void stopCapturing() {
    IOUtils.closeQuietly(grpcClientLog, shortCircuitClientLog);
  }

  @Override
  int getDatanodeCount() {
    return getRepConfig().getRequiredNodes();
  }

  @Override
  ReplicationConfig getRepConfig() {
    return RatisReplicationConfig.getInstance(ONE);
  }

  @Test
  void testFallbackToGrpc() throws Exception {
    try (OzoneClient client = getCluster().newClient()) {
      BucketForTesting bucket = BucketForTesting.newBuilder(client).build();

      debugShortCircuitRead();

      // create key
      String keyName = getNewKeyName();
      int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
      byte[] inputData = bucket.writeRandomBytes(keyName, getRepConfig(), dataLength);
      try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
        BlockInputStream block0Stream =
            (BlockInputStream)keyInputStream.getPartStreams().get(0);
        block0Stream.initialize();
        assertNotNull(block0Stream.getBlockFileInputStream());
        assertThat(shortCircuitClientLog.getOutput()).contains("XceiverClientShortCircuit is created");

        // stop XceiverServerDomainSocket server before client sends the second getBlockRequest to server
        XceiverServerSpi server = getCluster().getHddsDatanodes().get(0)
            .getDatanodeStateMachine().getContainer().getReadDomainSocketChannel();
        server.stop();
        BlockInputStream block1Stream = (BlockInputStream)keyInputStream.getPartStreams().get(1);
        try {
          block1Stream.initialize();
        } catch (IOException e) {
          assertThat(e.getMessage()).contains("DomainSocket stream is not open");
          assertThat(shortCircuitClientLog.getOutput())
              .contains("ReceiveResponseTask is closed due to java.io.EOFException");
        }
        assertNull(block1Stream.getBlockFileInputStream());
        // read whole key through Grpc channel
        byte[] data = new byte[dataLength];
        int readLen = keyInputStream.read(data);
        assertEquals(dataLength, readLen);
        assertArrayEquals(inputData, data);
        assertThat(grpcClientLog.getOutput()).contains("XceiverClientGrpc is created");
      }
    }
  }
}
