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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.freon.TestOmBucketReadWriteFileOps.ParameterBuilder;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OMLockMetrics;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for OmBucketReadWriteKeyOps.
 */
public abstract class TestOmBucketReadWriteKeyOps implements NonHATests.TestCase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmBucketReadWriteKeyOps.class);
  private OzoneClient client;

  @BeforeEach
  void setup() throws Exception {
    client = cluster().newClient();
  }

  @AfterEach
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  static List<ParameterBuilder> parameters() {
    return Arrays.asList(
        new ParameterBuilder()
            .setLength(16)
            .setTotalThreadCount(10)
            .setNumOfReadOperations(10)
            .setNumOfWriteOperations(5)
            .setReadThreadPercentage(80)
            .setCountForRead(10)
            .setCountForWrite(5),
        new ParameterBuilder()
            .setLength(32)
            .setTotalThreadCount(10)
            .setNumOfReadOperations(10)
            .setNumOfWriteOperations(5)
            .setReadThreadPercentage(70)
            .setCountForRead(10)
            .setCountForWrite(5),
        new ParameterBuilder()
            .setTotalThreadCount(15)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(3)
            .setCountForRead(5)
            .setCountForWrite(3),
        new ParameterBuilder()
            .setTotalThreadCount(10)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(3)
            .setCountForRead(5)
            .setCountForWrite(3)
            .setDataSize("64B")
            .setBufferSize(16),
        new ParameterBuilder()
            .setTotalThreadCount(10)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(0)
            .setCountForRead(5),
        new ParameterBuilder()
            .setTotalThreadCount(20)
            .setNumOfReadOperations(0)
            .setNumOfWriteOperations(5)
            .setCountForRead(0)
            .setCountForWrite(5)
    );
  }

  @ParameterizedTest(name = "Filesystem Paths Enabled: {0}")
  @MethodSource("parameters")
  void testOmBucketReadWriteKeyOps(ParameterBuilder parameterBuilder) throws Exception {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client,
        parameterBuilder.getVolumeName(),
        parameterBuilder.getBucketName(),
        parameterBuilder.getBucketArgs().setBucketLayout(BucketLayout.OBJECT_STORE).build()
    );

    long startTime = System.currentTimeMillis();
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    new Freon().getCmd().execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "obrwk",
        "-v", parameterBuilder.getVolumeName(),
        "-b", parameterBuilder.getBucketName(),
        "-k", String.valueOf(parameterBuilder.getCountForRead()),
        "-w", String.valueOf(parameterBuilder.getCountForWrite()),
        "-g", parameterBuilder.getDataSize(),
        "--buffer", String.valueOf(parameterBuilder.getBufferSize()),
        "-l", String.valueOf(parameterBuilder.getLength()),
        "-c", String.valueOf(parameterBuilder.getTotalThreadCount()),
        "-T", String.valueOf(parameterBuilder.getReadThreadPercentage()),
        "-R", String.valueOf(parameterBuilder.getNumOfReadOperations()),
        "-W", String.valueOf(parameterBuilder.getNumOfWriteOperations()),
        "-n", String.valueOf(1));
    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Total Execution Time: " + totalTime);

    LOG.info("Started verifying OM bucket read/write ops key generation...");
    verifyKeyCreation(parameterBuilder.getCountForRead(), bucket, "/readPath/");
    verifyKeyCreation(parameterBuilder.getExpectedWriteCount(), bucket, "/writePath/");

    verifyOMLockMetrics(cluster().getOzoneManager().getMetadataManager().getLock()
        .getOMLockMetrics());
  }

  private void verifyKeyCreation(int expectedCount, OzoneBucket bucket,
                                 String keyPrefix) throws IOException {
    int actual = 0;
    Iterator<? extends OzoneKey> ozoneKeyIterator = bucket.listKeys(keyPrefix);
    while (ozoneKeyIterator.hasNext()) {
      ozoneKeyIterator.next();
      ++actual;
    }
    assertEquals(expectedCount, actual, "Mismatch Count!");
  }

  private void verifyOMLockMetrics(OMLockMetrics omLockMetrics) {
    String readLockWaitingTimeMsStat =
        omLockMetrics.getReadLockWaitingTimeMsStat();
    LOG.info("Read Lock Waiting Time Stat: " + readLockWaitingTimeMsStat);
    LOG.info("Longest Read Lock Waiting Time (ms): " +
        omLockMetrics.getLongestReadLockWaitingTimeMs());
    int readWaitingSamples =
        Integer.parseInt(readLockWaitingTimeMsStat.split(" ")[2]);
    assertThat(readWaitingSamples).isGreaterThan(0);

    String readLockHeldTimeMsStat = omLockMetrics.getReadLockHeldTimeMsStat();
    LOG.info("Read Lock Held Time Stat: " + readLockHeldTimeMsStat);
    LOG.info("Longest Read Lock Held Time (ms): " +
        omLockMetrics.getLongestReadLockHeldTimeMs());
    int readHeldSamples =
        Integer.parseInt(readLockHeldTimeMsStat.split(" ")[2]);
    assertThat(readHeldSamples).isGreaterThan(0);

    String writeLockWaitingTimeMsStat =
        omLockMetrics.getWriteLockWaitingTimeMsStat();
    LOG.info("Write Lock Waiting Time Stat: " + writeLockWaitingTimeMsStat);
    LOG.info("Longest Write Lock Waiting Time (ms): " +
        omLockMetrics.getLongestWriteLockWaitingTimeMs());
    int writeWaitingSamples =
        Integer.parseInt(writeLockWaitingTimeMsStat.split(" ")[2]);
    assertThat(writeWaitingSamples).isGreaterThan(0);

    String writeLockHeldTimeMsStat = omLockMetrics.getWriteLockHeldTimeMsStat();
    LOG.info("Write Lock Held Time Stat: " + writeLockHeldTimeMsStat);
    LOG.info("Longest Write Lock Held Time (ms): " +
        omLockMetrics.getLongestWriteLockHeldTimeMs());
    int writeHeldSamples =
        Integer.parseInt(writeLockHeldTimeMsStat.split(" ")[2]);
    assertThat(writeHeldSamples).isGreaterThan(0);
  }

}
