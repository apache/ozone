/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc.read;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientShortCircuit;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.hdds.scm.storage.LocalChunkInputStream;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.om.TestBucket;
import org.apache.ozone.test.GenericTestUtils;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;

/**
 * Tests {@link LocalChunkInputStream}.
 */
public class TestShortCircuitChunkInputStream extends TestChunkInputStream {

  @TempDir
  private File dir;

  @Override
  int getDatanodeCount() {
    return 1;
  }

  @Override
  void setCustomizedProperties(OzoneConfiguration configuration) {
    OzoneClientConfig clientConfig = configuration.getObject(OzoneClientConfig.class);
    clientConfig.setShortCircuit(true);
    configuration.setFromObject(clientConfig);
    configuration.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-socket").getAbsolutePath());
    GenericTestUtils.setLogLevel(XceiverClientShortCircuit.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(XceiverClientGrpc.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(LocalChunkInputStream.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockInputStream.LOG, Level.DEBUG);
  }

  @Override
  ReplicationConfig getRepConfig() {
    return RatisReplicationConfig.getInstance(ONE);
  }


  /**
   * Run the tests as a single test method to avoid needing a new mini-cluster
   * for each test.
   */
  @ContainerLayoutTestInfo.ContainerTest
  @Override
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  void testAll(ContainerLayoutVersion layout) throws Exception {
    try (MiniOzoneCluster cluster = newCluster(layout)) {
      cluster.waitForClusterToBeReady();
      assumeTrue(DomainSocketFactory.getInstance(cluster.getConf()).isServiceReady());

      try (OzoneClient client = cluster.newClient()) {
        TestBucket bucket = TestBucket.newBuilder(client).build();
        GenericTestUtils.LogCapturer logCapturer1 =
            GenericTestUtils.LogCapturer.captureLogs(LocalChunkInputStream.LOG);
        GenericTestUtils.LogCapturer logCapturer2 =
            GenericTestUtils.LogCapturer.captureLogs(XceiverClientShortCircuit.LOG);
        GenericTestUtils.LogCapturer logCapturer3 =
            GenericTestUtils.LogCapturer.captureLogs(BlockInputStream.LOG);
        GenericTestUtils.LogCapturer logCapturer4 =
            GenericTestUtils.LogCapturer.captureLogs(XceiverClientGrpc.LOG);
        testChunkReadBuffers(bucket);
        testBufferRelease(bucket);
        testCloseReleasesBuffers(bucket);
        assertTrue(logCapturer1.getOutput().contains("ShortCircuitChunkInputStream is created"));
        assertTrue(logCapturer2.getOutput().contains("XceiverClientShortCircuit is created"));
        assertTrue((logCapturer3.getOutput().contains("Get the FileInputStream of block")));
        assertFalse(logCapturer4.getOutput().contains("XceiverClientGrpc is created"));
      }
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  void testFallbackToGrpc(ContainerLayoutVersion layout) throws Exception {
    try (MiniOzoneCluster cluster = newCluster(layout)) {
      cluster.waitForClusterToBeReady();
      assumeTrue(DomainSocketFactory.getInstance(cluster.getConf()).isServiceReady());

      try (OzoneClient client = cluster.newClient()) {
        TestBucket bucket = TestBucket.newBuilder(client).build();
        GenericTestUtils.LogCapturer logCapturer1 =
            GenericTestUtils.LogCapturer.captureLogs(XceiverClientShortCircuit.LOG);
        GenericTestUtils.LogCapturer logCapturer2 =
            GenericTestUtils.LogCapturer.captureLogs(XceiverClientGrpc.LOG);

        // create key
        String keyName = getNewKeyName();
        int dataLength = (2 * BLOCK_SIZE) + (CHUNK_SIZE);
        byte[] inputData = bucket.writeRandomBytes(keyName, getRepConfig(), dataLength);
        try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
          BlockInputStream block0Stream =
              (BlockInputStream)keyInputStream.getPartStreams().get(0);
          block0Stream.initialize();
          assertNotNull(block0Stream.getBlockFileInputStream());
          assertTrue(logCapturer1.getOutput().contains("XceiverClientShortCircuit is created"));
          // stop XceiverServerDomainSocket
          XceiverServerSpi server = cluster.getHddsDatanodes().get(0)
              .getDatanodeStateMachine().getContainer().getReadDomainSocketChannel();
          server.stop();
          BlockInputStream block1Stream = (BlockInputStream)keyInputStream.getPartStreams().get(1);
          try {
            block1Stream.initialize();
          } catch (IOException e) {
            assertTrue(e.getMessage().contains("DomainSocket stream is not open"));
            assertTrue(logCapturer1.getOutput().contains("ReceiveResponseTask is closed due to java.io.EOFException"));
          }
          // read whole key
          byte[] data = new byte[dataLength];
          int readLen = keyInputStream.read(data);
          assertEquals(dataLength, readLen);
          assertArrayEquals(inputData, data);
          assertTrue(logCapturer2.getOutput().contains("XceiverClientGrpc is created"));
        }
      }
    }
  }
}
