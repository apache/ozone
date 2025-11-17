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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests the metrics of XceiverClient.
 */
public class TestXceiverClientMetrics {

  // only for testing
  private volatile boolean breakFlag;
  private CountDownLatch latch;

  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(config).build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @AfterAll
  public static void shutdown() {
    cluster.shutdown();
  }

  @Test
  @Flaky("HDDS-11646")
  public void testMetrics(@TempDir Path metaDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_METADATA_DIR_NAME, metaDir.toString());

    try (XceiverClientManager clientManager = new XceiverClientManager(conf)) {

      ContainerWithPipeline container = storageContainerLocationClient
          .allocateContainer(
              SCMTestUtils.getReplicationType(conf),
              SCMTestUtils.getReplicationFactor(conf),
              OzoneConsts.OZONE);
      XceiverClientSpi client = clientManager
          .acquireClient(container.getPipeline());

      ContainerCommandRequestProto request = ContainerTestHelper
          .getCreateContainerRequest(
              container.getContainerInfo().getContainerID(),
              container.getPipeline());
      client.sendCommand(request);

      MetricsRecordBuilder containerMetrics = getMetrics(
          XceiverClientMetrics.SOURCE_NAME);
      // Above request command is in a synchronous way, so there will be no
      // pending requests.
      assertCounter("PendingOps", 0L, containerMetrics);
      assertCounter("numPendingCreateContainer", 0L, containerMetrics);
      // the counter value of average latency metric should be increased
      assertCounter("CreateContainerLatencyNumOps", 1L, containerMetrics);

      breakFlag = false;
      latch = new CountDownLatch(1);

      int numRequest = 10;
      List<CompletableFuture<ContainerCommandResponseProto>> computeResults
          = new ArrayList<>();
      // start new thread to send async requests
      Thread sendThread = new Thread(() -> {
        while (!breakFlag) {
          try {
            // use async interface for testing pending metrics
            for (int i = 0; i < numRequest; i++) {
              BlockID blockID = ContainerTestHelper.
                  getTestBlockID(container.getContainerInfo().getContainerID());
              ContainerProtos.ContainerCommandRequestProto smallFileRequest;

              smallFileRequest = ContainerTestHelper.getWriteSmallFileRequest(
                  client.getPipeline(), blockID, 1024);
              CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
                  response =
                  client.sendCommandAsync(smallFileRequest).getResponse();
              computeResults.add(response);
            }

            Thread.sleep(1000);
          } catch (Exception ignored) {
          }
        }

        latch.countDown();
      });
      sendThread.start();

      GenericTestUtils.waitFor(() -> {
        // check if pending metric count is increased
        MetricsRecordBuilder metric =
            getMetrics(XceiverClientMetrics.SOURCE_NAME);
        long pendingOps = getLongCounter("PendingOps", metric);
        long pendingPutSmallFileOps =
            getLongCounter("numPendingPutSmallFile", metric);

        if (pendingOps > 0 && pendingPutSmallFileOps > 0) {
          // reset break flag
          breakFlag = true;
          return true;
        } else {
          return false;
        }
      }, 100, 60000);

      // blocking until we stop sending async requests
      latch.await();
      // Wait for all futures being done.
      GenericTestUtils.waitFor(() -> {
        for (CompletableFuture future : computeResults) {
          if (!future.isDone()) {
            return false;
          }
        }

        return true;
      }, 100, 60000);

      // the counter value of pending metrics should be decreased to 0
      containerMetrics = getMetrics(XceiverClientMetrics.SOURCE_NAME);
      assertCounter("PendingOps", 0L, containerMetrics);
      assertCounter("numPendingPutSmallFile", 0L, containerMetrics);

      clientManager.releaseClient(client, false);
    }
  }
}
