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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

/**
 * Tests pipeline close logs.
 */
@Timeout(300)
public class TestPipelineCloseLogsFlood {
  private static final String FLOOD_TOKEN = "pipeline Action CLOSE";
  private static final String DATANODE_SLOWNESS_TIMEOUT = "hdds.ratis.raft.server.rpc.slowness.timeout";
  private static final String NO_LEADER_TIMEOUT = "hdds.ratis.raft.server.notification.no-leader.timeout";
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    // Make follower‑slowness detection fire quickly so that the log floods
    conf.setTimeDuration(DATANODE_SLOWNESS_TIMEOUT, 10, TimeUnit.SECONDS);
    conf.setTimeDuration(NO_LEADER_TIMEOUT, 10, TimeUnit.SECONDS);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    client.close();
  }

  @Test
  public void testPipelineCloseLogFloodDoesntOccur() throws Exception {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.captureLogs(XceiverServerRatis.class);
    GenericTestUtils.setLogLevel(XceiverServerRatis.class, Level.ERROR);

    client.getObjectStore().createVolume(VOLUME_NAME);
    client.getObjectStore().getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
    OzoneBucket ozoneBucket =  client.getObjectStore().getVolume(VOLUME_NAME).getBucket(BUCKET_NAME);
    ReplicationConfig config = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

    try (OutputStream out = ozoneBucket.createKey("key", 1024, config, new HashMap<>())) {
      out.write(new byte[1024]);
    }
    // Kill one follower DN so that the pipeline becomes unhealthy
    cluster.shutdownHddsDatanode(1);
    // Wait (30 sec > a few heartbeat cycles) for multiple slowness callbacks
    Thread.sleep(30_000L);
    logCapturer.stopCapturing();
    int occurrences = StringUtils.countMatches(logCapturer.getOutput(), FLOOD_TOKEN);
    // We expect many duplicates when the bug is present. A threshold of 5 is safe.
    assertThat(occurrences).isGreaterThan(0);
    assertThat(occurrences).isLessThan(5);
  }
}
