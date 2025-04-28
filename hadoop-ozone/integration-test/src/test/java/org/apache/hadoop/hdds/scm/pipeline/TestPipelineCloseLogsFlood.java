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
import java.time.Duration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests pipeline close logs.
 */
@Timeout(300)
public class TestPipelineCloseLogsFlood {
  private static final String FLOOD_TOKEN = "pipeline Action CLOSE";
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String KEY_NAME = "key1";

  private MiniOzoneCluster cluster;
  private OzoneClient client;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig = conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setFollowerSlownessTimeout(Duration.ofSeconds(10));
    ratisServerConfig.setNoLeaderTimeout(Duration.ofMinutes(5));
    conf.setFromObject(ratisServerConfig);
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

    client.getObjectStore().createVolume(VOLUME_NAME);
    client.getObjectStore().getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
    OzoneBucket ozoneBucket =  client.getObjectStore().getVolume(VOLUME_NAME).getBucket(BUCKET_NAME);

    TestDataUtil.createKey(ozoneBucket, KEY_NAME, new byte[1024]);
    // Kill one follower DN so that the pipeline becomes unhealthy
    cluster.shutdownHddsDatanode(1);
    // wait for time > follower slowness timeout
    Thread.sleep(13_000);
    logCapturer.stopCapturing();
    int occurrences = StringUtils.countMatches(logCapturer.getOutput(), FLOOD_TOKEN);
    //Follower slowness will happen for 2 pipelines since we are shutting down one node
    assertThat(occurrences).isEqualTo(2);
  }
}
