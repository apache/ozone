/**
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

package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;

/**
 * Tests Freon, with MiniOzoneCluster.
 */
public class TestOMSnapshotDAG {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOMSnapshotDAG.class);

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static ObjectStore store;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   */
  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(3));
    conf.setFromObject(raftClientConfig);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();

    store = cluster.getClient().getObjectStore();

    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.INFO);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.INFO);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      LOG.warn("Waiting for an extra 10 seconds before shutting down...");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      cluster.shutdown();
    }
  }

  @Test
  void testZeroSizeKey() throws IOException {

    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "6000",
        "--num-of-threads", "1",
        "--key-size", "0",
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    Assert.assertEquals(6000L, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(6000L,
        randomKeyGenerator.getSuccessfulValidationCount());

    List<OmVolumeArgs> volList = cluster.getOzoneManager()
        .listAllVolumes("", "", 10);
    System.out.println(volList);
    final String volumeName = volList.stream().filter(e ->
        !e.getVolume().equals(OZONE_S3_VOLUME_NAME_DEFAULT))  // Ignore s3v vol
        .collect(Collectors.toList()).get(0).getVolume();
    List<OmBucketInfo> bucketList =
        cluster.getOzoneManager().listBuckets(volumeName, "", "", 10);
    System.out.println(bucketList);
    final String bucketName = bucketList.get(0).getBucketName();

    // Create snapshot
    String resp = store.createSnapshot(volumeName, bucketName, "snap1");
    System.out.println(resp);

    final OzoneVolume volume = store.getVolume(volumeName);
    final OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 6000; i++) {
      bucket.createKey("b_" + i, 0).close();
    }

    resp = store.createSnapshot(volumeName, bucketName, "snap3");
    System.out.println(resp);
  }

}