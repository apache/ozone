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

package org.apache.hadoop.ozone.freon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests Freon, with MiniOzoneCluster and validate data.
 */
public abstract class TestDataValidate {

  private static MiniOzoneCluster cluster = null;

  static void startCluster(OzoneConfiguration conf) throws Exception {
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

    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 8);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
            180000);
  }

  static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void ratisTestLargeKey() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--key-size", "20MB",
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(0, randomKeyGenerator.getUnsuccessfulValidationCount());
  }

  @Test
  public void validateWriteTest() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "2",
        "--num-of-buckets", "5",
        "--num-of-keys", "10",
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
    assertTrue(randomKeyGenerator.getValidateWrites());
    assertNotEquals(0, randomKeyGenerator.getTotalKeysValidated());
    assertNotEquals(0, randomKeyGenerator.getSuccessfulValidationCount());
    assertEquals(0, randomKeyGenerator.getUnsuccessfulValidationCount());
  }
}
