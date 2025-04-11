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

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.lock.OMLockMetrics;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for OmBucketReadWriteFileOps.
 */
public abstract class TestOmBucketReadWriteFileOps implements NonHATests.TestCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmBucketReadWriteFileOps.class);

  static List<ParameterBuilder> parameters() {
    return Arrays.asList(
        new ParameterBuilder()
            .setTotalThreadCount(10)
            .setNumOfReadOperations(10)
            .setNumOfWriteOperations(5)
            .setReadThreadPercentage(70)
            .setCountForRead(10)
            .setCountForWrite(5),
        new ParameterBuilder()
            .setPrefixFilePath("/dir1/dir2/dir3")
            .setTotalThreadCount(10)
            .setNumOfReadOperations(10)
            .setNumOfWriteOperations(5)
            .setReadThreadPercentage(80)
            .setBufferSize(128)
            .setCountForRead(10)
            .setCountForWrite(5),
        new ParameterBuilder()
            .setPrefixFilePath("/")
            .setTotalThreadCount(15)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(3)
            .setDataSize("128B")
            .setCountForRead(5)
            .setCountForWrite(3),
        new ParameterBuilder()
            .setPrefixFilePath("/dir1/")
            .setTotalThreadCount(10)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(3)
            .setCountForRead(5)
            .setCountForWrite(3)
            .setDataSize("64B")
            .setBufferSize(16),
        new ParameterBuilder()
            .setPrefixFilePath("/dir1/dir2/dir3")
            .setTotalThreadCount(10)
            .setNumOfReadOperations(5)
            .setNumOfWriteOperations(0)
            .setCountForRead(5),
        new ParameterBuilder()
            .setLength(64)
            .setPrefixFilePath("/dir1/dir2/dir3/dir4")
            .setTotalThreadCount(20)
            .setNumOfReadOperations(0)
            .setNumOfWriteOperations(5)
            .setCountForRead(0)
            .setCountForWrite(5)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void testOmBucketReadWriteFileOps(ParameterBuilder parameterBuilder) throws Exception {
    try (OzoneClient client = cluster().newClient()) {
      TestDataUtil.createVolumeAndBucket(client,
          parameterBuilder.getVolumeName(),
          parameterBuilder.getBucketName(),
          parameterBuilder.getBucketArgs().build()
      );
    }

    String rootPath = "o3fs://" + parameterBuilder.getBucketName() + "." +
            parameterBuilder.getVolumeName() + parameterBuilder.getPrefixFilePath();
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    new Freon().getCmd().execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "obrwf",
        "-P", rootPath,
        "-r", String.valueOf(parameterBuilder.getCountForRead()),
        "-w", String.valueOf(parameterBuilder.getCountForWrite()),
        "-g", parameterBuilder.getDataSize(),
        "--buffer", String.valueOf(parameterBuilder.getBufferSize()),
        "-l", String.valueOf(parameterBuilder.getLength()),
        "-c", String.valueOf(parameterBuilder.getTotalThreadCount()),
        "-T", String.valueOf(parameterBuilder.getReadThreadPercentage()),
        "-R", String.valueOf(parameterBuilder.getNumOfReadOperations()),
        "-W", String.valueOf(parameterBuilder.getNumOfWriteOperations()),
        "-n", String.valueOf(1)
    );
    LOG.info("Started verifying OM bucket read/write ops file generation...");
    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath), cluster().getConf())) {
      Path rootDir = new Path(rootPath.concat(OzoneConsts.OM_KEY_PREFIX));
      FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
      verifyFileCreation(2, fileStatuses, true);

      Path readDir = new Path(rootPath.concat("/readPath"));
      FileStatus[] readFileStatuses = fileSystem.listStatus(readDir);
      verifyFileCreation(parameterBuilder.getCountForRead(), readFileStatuses,
          false);

      Path writeDir = new Path(rootPath.concat("/writePath"));
      FileStatus[] writeFileStatuses = fileSystem.listStatus(writeDir);
      verifyFileCreation(parameterBuilder.getExpectedWriteCount(), writeFileStatuses, false);

      verifyOMLockMetrics(cluster().getOzoneManager().getMetadataManager().getLock()
          .getOMLockMetrics());
    }
  }

  private void verifyFileCreation(int expectedCount, FileStatus[] fileStatuses,
                          boolean checkDirectoryCount) {
    int actual = 0;
    if (checkDirectoryCount) {
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isDirectory()) {
          ++actual;
        }
      }
    } else {
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isFile()) {
          ++actual;
        }
      }
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
    assertThat(readWaitingSamples).isPositive();

    String readLockHeldTimeMsStat = omLockMetrics.getReadLockHeldTimeMsStat();
    LOG.info("Read Lock Held Time Stat: " + readLockHeldTimeMsStat);
    LOG.info("Longest Read Lock Held Time (ms): " +
        omLockMetrics.getLongestReadLockHeldTimeMs());
    int readHeldSamples =
        Integer.parseInt(readLockHeldTimeMsStat.split(" ")[2]);
    assertThat(readHeldSamples).isPositive();

    String writeLockWaitingTimeMsStat =
        omLockMetrics.getWriteLockWaitingTimeMsStat();
    LOG.info("Write Lock Waiting Time Stat: " + writeLockWaitingTimeMsStat);
    LOG.info("Longest Write Lock Waiting Time (ms): " +
        omLockMetrics.getLongestWriteLockWaitingTimeMs());
    int writeWaitingSamples =
        Integer.parseInt(writeLockWaitingTimeMsStat.split(" ")[2]);
    assertThat(writeWaitingSamples).isPositive();

    String writeLockHeldTimeMsStat = omLockMetrics.getWriteLockHeldTimeMsStat();
    LOG.info("Write Lock Held Time Stat: " + writeLockHeldTimeMsStat);
    LOG.info("Longest Write Lock Held Time (ms): " +
        omLockMetrics.getLongestWriteLockHeldTimeMs());
    int writeHeldSamples =
        Integer.parseInt(writeLockHeldTimeMsStat.split(" ")[2]);
    assertThat(writeHeldSamples).isPositive();
  }

  static class ParameterBuilder {

    private static final String BUCKET_NAME = "bucket1";

    private final String volumeName = "vol-" + UUID.randomUUID();
    private final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    private String prefixFilePath = "/dir1/dir2";
    private int countForRead = 100;
    private int countForWrite = 10;
    private String dataSize = "256B";
    private int bufferSize = 64;
    private int length = 10;
    private int totalThreadCount = 100;
    private int readThreadPercentage = 90;
    private int numOfReadOperations = 50;
    private int numOfWriteOperations = 10;

    int getExpectedWriteCount() {
      int readThreadCount = (getReadThreadPercentage() * getTotalThreadCount()) / 100;
      int writeThreadCount = getTotalThreadCount() - readThreadCount;
      return writeThreadCount * getCountForWrite() * getNumOfWriteOperations();
    }

    ParameterBuilder setPrefixFilePath(String newValue) {
      prefixFilePath = newValue;
      return this;
    }

    ParameterBuilder setCountForRead(int newValue) {
      countForRead = newValue;
      return this;
    }

    ParameterBuilder setCountForWrite(int newValue) {
      countForWrite = newValue;
      return this;
    }

    ParameterBuilder setDataSize(String newValue) {
      dataSize = newValue;
      return this;
    }

    ParameterBuilder setBufferSize(int newValue) {
      bufferSize = newValue;
      return this;
    }

    ParameterBuilder setLength(int newValue) {
      length = newValue;
      return this;
    }

    ParameterBuilder setTotalThreadCount(int newValue) {
      totalThreadCount = newValue;
      return this;
    }

    ParameterBuilder setReadThreadPercentage(int newValue) {
      readThreadPercentage = newValue;
      return this;
    }

    ParameterBuilder setNumOfReadOperations(int newValue) {
      numOfReadOperations = newValue;
      return this;
    }

    ParameterBuilder setNumOfWriteOperations(int newValue) {
      numOfWriteOperations = newValue;
      return this;
    }

    public String getVolumeName() {
      return volumeName;
    }

    public String getBucketName() {
      return BUCKET_NAME;
    }

    public String getPrefixFilePath() {
      return prefixFilePath;
    }

    public BucketArgs.Builder getBucketArgs() {
      return bucketArgs;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public String getDataSize() {
      return dataSize;
    }

    public int getCountForRead() {
      return countForRead;
    }

    public int getCountForWrite() {
      return countForWrite;
    }

    public int getLength() {
      return length;
    }

    public int getTotalThreadCount() {
      return totalThreadCount;
    }

    public int getReadThreadPercentage() {
      return readThreadPercentage;
    }

    public int getNumOfReadOperations() {
      return numOfReadOperations;
    }

    public int getNumOfWriteOperations() {
      return numOfWriteOperations;
    }
  }
}
