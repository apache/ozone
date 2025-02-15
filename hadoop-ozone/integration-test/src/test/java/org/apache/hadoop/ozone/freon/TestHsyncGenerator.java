/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Duration;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests Freon HsyncGenerator with MiniOzoneCluster and validate data.
 */
public class TestHsyncGenerator {
  private static MiniOzoneCluster cluster = null;

  private static void startCluster(OzoneConfiguration conf) throws Exception {
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);
    conf.set(OZONE_FS_HSYNC_ENABLED, "true");

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
  }

  static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    startCluster(conf);
  }

  @AfterAll
  public static void shutdown() {
    shutdownCluster();
  }

  @Test
  public void test() throws IOException {
    HsyncGenerator randomKeyGenerator =
        new HsyncGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);

    String volumeName = "vol1";
    String bucketName = "bucket1";
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);

      String rootPath = String.format("%s://%s/%s/%s/", OZONE_OFS_URI_SCHEME,
          cluster.getConf().get(OZONE_OM_ADDRESS_KEY), volumeName, bucketName);

      int exitCode = cmd.execute(
          "--path", rootPath,
          "--bytes-per-write", "8",
          "--writes-per-transaction", "64",
          "-t", "5",
          "-n", "100");
      assertEquals(0, exitCode);
    }
  }
}
