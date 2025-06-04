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

import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests Freon, with MiniOzoneCluster.
 */
public abstract class TestRandomKeyGenerator implements NonHATests.TestCase {

  @Test
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
  void testDefaultReplication() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster().getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "2",
        "--num-of-buckets", "3",
        "--num-of-keys", "4",
        "--validate-writes"
    );

    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(6, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(24, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(24, randomKeyGenerator.getTotalKeysValidated());
    assertEquals(24, randomKeyGenerator.getSuccessfulValidationCount());
    assertEquals(0, randomKeyGenerator.getUnsuccessfulValidationCount());
    randomKeyGenerator.printStats(System.out);
  }

  @Test
  void testECKey() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster().getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--replication", "rs-3-2-1024k",
        "--type", "EC",
        "--validate-writes"
    );

    assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(1, randomKeyGenerator.getTotalKeysValidated());
    assertEquals(1, randomKeyGenerator.getSuccessfulValidationCount());
    assertEquals(0, randomKeyGenerator.getUnsuccessfulValidationCount());
  }

  @Test
  void testMultiThread() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster().getConf());
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
        new RandomKeyGenerator(cluster().getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--num-of-threads", "1",
        "--key-size", "2.01GB",
        "--factor", "THREE",
        "--type", "RATIS"
    );

    assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  void testZeroSizeKey() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster().getConf());
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
    assertEquals(1, randomKeyGenerator.getTotalKeysValidated());
    assertEquals(1, randomKeyGenerator.getSuccessfulValidationCount());
    assertEquals(0, randomKeyGenerator.getUnsuccessfulValidationCount());
  }

  @Test
  void testThreadPoolSize() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster().getConf());
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
        new RandomKeyGenerator(cluster().getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "2",
        "--num-of-buckets", "3",
        "--num-of-keys", "2",
        "--num-of-threads", "10",
        "--factor", "THREE",
        "--type", "RATIS",
        "--clean-objects"
    );

    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(6, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(12, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(2, randomKeyGenerator.getNumberOfVolumesCleaned());
    assertEquals(6, randomKeyGenerator.getNumberOfBucketsCleaned());
  }

  @Test
  void testBucketLayoutOption() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster().getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "2",
        "--bucket-layout", "OBJECT_STORE"
    );

    assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
    assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    assertEquals(2, randomKeyGenerator.getNumberOfKeysAdded());
    assertEquals(1, randomKeyGenerator.getBucketMapSize());

    // Fetch the bucket and check its layout
    OzoneBucket bucket = randomKeyGenerator.getBucket(0);
    assertEquals(BucketLayout.OBJECT_STORE, bucket.getBucketLayout());
  }
}
