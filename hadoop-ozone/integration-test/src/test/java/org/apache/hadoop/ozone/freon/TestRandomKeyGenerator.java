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

import java.time.Duration;

import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.ozone.MiniOzoneCluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import picocli.CommandLine;

import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests Freon, with MiniOzoneCluster.
 */
public class TestRandomKeyGenerator {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeAll
  static void init() throws Exception {
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
  }

  @AfterAll
  static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(5)
  void singleFailedAttempt() {
    BaseFreonGenerator subject = new BaseFreonGenerator();
    subject.setThreadNo(2);
    subject.setTestNo(1);
    subject.init();

    assertThrows(RuntimeException.class, () -> subject.runTests(
        n -> {
          waitFor(subject::isCompleted, 100, 3000);
          throw new RuntimeException("fail");
        }
    ));
    assertEquals(1, subject.getFailureCount());
  }

  @Test
  void testDefault() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "2",
        "--num-of-buckets", "5",
        "--num-of-keys", "10");

    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
    randomKeyGenerator.printStats(System.out);
  }

  @Test
  void testECKey() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "2",
        "--num-of-buckets", "5",
        "--num-of-keys", "10",
        "--replication", "rs-3-2-1024k",
        "--type", "EC"
    );

    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  void testMultiThread() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "10",
        "--num-of-buckets", "1",
        "--num-of-keys", "10",
        "--num-of-threads", "10",
        "--key-size", "10KB",
        "--factor", "THREE",
        "--type", "RATIS"
    );

    assertEquals(10, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  void testRatisKey() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "10",
        "--num-of-buckets", "1",
        "--num-of-keys", "10",
        "--num-of-threads", "10",
        "--key-size", "10KB",
        "--factor", "THREE",
        "--type", "RATIS"
    );

    assertEquals(10, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  void testKeyLargerThan2GB() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--num-of-threads", "1",
        "--key-size", "2.01GB",
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(1, randomKeyGenerator.getSuccessfulValidationCount());
  }

  @Test
  void testZeroSizeKey() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--num-of-threads", "1",
        "--key-size", "0",
        "--factor", "THREE",
        "--type", "RATIS",
        "--validate-writes"
    );

    assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(1, randomKeyGenerator.getSuccessfulValidationCount());
  }

  @Test
  void testThreadPoolSize() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--num-of-threads", "10",
        "--factor", "THREE",
        "--type", "RATIS"
    );

    assertEquals(10, randomKeyGenerator.getThreadPoolSize());
    assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  void cleanObjectsTest() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "2",
        "--num-of-buckets", "5",
        "--num-of-keys", "10",
        "--num-of-threads", "10",
        "--factor", "THREE",
        "--type", "RATIS",
        "--clean-objects"
    );

    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCleaned());
    assertEquals(10, randomKeyGenerator.getNumberOfBucketsCleaned());
  }
}
