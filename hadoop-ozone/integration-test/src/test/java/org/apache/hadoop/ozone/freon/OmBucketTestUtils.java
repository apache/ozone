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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.om.lock.OMLockMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util Class for OmBucketReadWriteKeyTests.
 */
public final class OmBucketTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(OmBucketTestUtils.class);

  private OmBucketTestUtils() {
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

    private String description;

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

    ParameterBuilder setDescription(String newValue) {
      description = newValue;
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

    public String getDescription() {
      return description;
    }

    @Override
    public String toString() {
      return String.format("%s", description);
    }
  }

  public static  void verifyOMLockMetrics(OMLockMetrics omLockMetrics) {
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
