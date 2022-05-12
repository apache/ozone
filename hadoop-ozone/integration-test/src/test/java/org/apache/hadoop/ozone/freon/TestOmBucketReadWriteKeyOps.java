/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OMLockMetrics;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Test for OmBucketReadWriteKeyOps.
 */
public class TestOmBucketReadWriteKeyOps {

  // TODO: Remove code duplication of TestOmBucketReadWriteKeyOps with
  //  TestOmBucketReadWriteFileOps.

  private String path;
  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster = null;
  private ObjectStore store = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmBucketReadWriteKeyOps.class);

  @Before
  public void setup() {
    path = GenericTestUtils
        .getTempPath(TestHadoopDirTreeGenerator.class.getSimpleName());
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    File baseDir = new File(path);
    baseDir.mkdirs();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  private void startCluster() throws Exception {
    conf = getOzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.OBJECT_STORE.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();

    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
  }

  private OzoneConfiguration getOzoneConfiguration() {
    return new OzoneConfiguration();
  }

  @Test
  public void testOmBucketReadWriteKeyOps() throws Exception {
    try {
      startCluster();
      FileOutputStream out = FileUtils.openOutputStream(new File(path,
          "conf"));
      cluster.getConf().writeXml(out);
      out.getFD().sync();
      out.close();

      verifyFreonCommand(new ParameterBuilder().setTotalThreadCount(10)
          .setNumOfReadOperations(10).setNumOfWriteOperations(5)
          .setKeyCountForRead(10).setKeyCountForWrite(5));
      verifyFreonCommand(
          new ParameterBuilder().setVolumeName("vol2").setBucketName("bucket1")
              .setTotalThreadCount(10).setNumOfReadOperations(10)
              .setNumOfWriteOperations(5).setKeyCountForRead(10)
              .setKeyCountForWrite(5));
      verifyFreonCommand(
          new ParameterBuilder().setVolumeName("vol3").setBucketName("bucket1")
              .setTotalThreadCount(15).setNumOfReadOperations(5)
              .setNumOfWriteOperations(3).setKeyCountForRead(5)
              .setKeyCountForWrite(3));
      verifyFreonCommand(
          new ParameterBuilder().setVolumeName("vol4").setBucketName("bucket1")
              .setTotalThreadCount(10).setNumOfReadOperations(5)
              .setNumOfWriteOperations(3).setKeyCountForRead(5)
              .setKeyCountForWrite(3).setKeySizeInBytes(64)
              .setBufferSize(16));
      verifyFreonCommand(
          new ParameterBuilder().setVolumeName("vol5").setBucketName("bucket1")
              .setTotalThreadCount(10).setNumOfReadOperations(5)
              .setNumOfWriteOperations(0).setKeyCountForRead(5));
      verifyFreonCommand(
          new ParameterBuilder().setVolumeName("vol6").setBucketName("bucket1")
              .setTotalThreadCount(20).setNumOfReadOperations(0)
              .setNumOfWriteOperations(5).setKeyCountForRead(0)
              .setKeyCountForWrite(5));

    } finally {
      shutdown();
    }
  }

  private void verifyFreonCommand(ParameterBuilder parameterBuilder)
      throws IOException {
    store.createVolume(parameterBuilder.volumeName);
    OzoneVolume volume = store.getVolume(parameterBuilder.volumeName);
    volume.createBucket(parameterBuilder.bucketName);
    OzoneBucket bucket = volume.getBucket(parameterBuilder.bucketName);
    String confPath = new File(path, "conf").getAbsolutePath();

    long startTime = System.currentTimeMillis();
    new Freon().execute(
        new String[]{"-conf", confPath, "obrwk",
            "-v", parameterBuilder.volumeName,
            "-b", parameterBuilder.bucketName,
            "-k", String.valueOf(parameterBuilder.keyCountForRead),
            "-w", String.valueOf(parameterBuilder.keyCountForWrite),
            "-g", String.valueOf(parameterBuilder.keySizeInBytes),
            "--buffer", String.valueOf(parameterBuilder.bufferSize),
            "-l", String.valueOf(parameterBuilder.length),
            "-c", String.valueOf(parameterBuilder.totalThreadCount),
            "-T", String.valueOf(parameterBuilder.readThreadPercentage),
            "-R", String.valueOf(parameterBuilder.numOfReadOperations),
            "-W", String.valueOf(parameterBuilder.numOfWriteOperations),
            "-n", String.valueOf(1)});
    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Total Execution Time: " + totalTime);

    LOG.info("Started verifying OM bucket read/write ops key generation...");
    verifyKeyCreation(parameterBuilder.keyCountForRead, bucket, "/readPath/");

    int readThreadCount = (parameterBuilder.readThreadPercentage *
        parameterBuilder.totalThreadCount) / 100;
    int writeThreadCount = parameterBuilder.totalThreadCount - readThreadCount;
    verifyKeyCreation(writeThreadCount * parameterBuilder.keyCountForWrite *
        parameterBuilder.numOfWriteOperations, bucket, "/writePath/");

    verifyOMLockMetrics(cluster.getOzoneManager().getMetadataManager().getLock()
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
    Assert.assertEquals("Mismatch Count!", expectedCount, actual);
  }

  private void verifyOMLockMetrics(OMLockMetrics omLockMetrics) {
    String readLockWaitingTimeMsStat =
        omLockMetrics.getReadLockWaitingTimeMsStat();
    LOG.info("Read Lock Waiting Time Stat: " + readLockWaitingTimeMsStat);
    LOG.info("Longest Read Lock Waiting Time (ms): " +
        omLockMetrics.getLongestReadLockWaitingTimeMs());
    int readWaitingSamples =
        Integer.parseInt(readLockWaitingTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Read Lock Waiting Samples should be positive",
        readWaitingSamples > 0);

    String readLockHeldTimeMsStat = omLockMetrics.getReadLockHeldTimeMsStat();
    LOG.info("Read Lock Held Time Stat: " + readLockHeldTimeMsStat);
    LOG.info("Longest Read Lock Held Time (ms): " +
        omLockMetrics.getLongestReadLockHeldTimeMs());
    int readHeldSamples =
        Integer.parseInt(readLockHeldTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Read Lock Held Samples should be positive",
        readHeldSamples > 0);

    String writeLockWaitingTimeMsStat =
        omLockMetrics.getWriteLockWaitingTimeMsStat();
    LOG.info("Write Lock Waiting Time Stat: " + writeLockWaitingTimeMsStat);
    LOG.info("Longest Write Lock Waiting Time (ms): " +
        omLockMetrics.getLongestWriteLockWaitingTimeMs());
    int writeWaitingSamples =
        Integer.parseInt(writeLockWaitingTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Write Lock Waiting Samples should be positive",
        writeWaitingSamples > 0);

    String writeLockHeldTimeMsStat = omLockMetrics.getWriteLockHeldTimeMsStat();
    LOG.info("Write Lock Held Time Stat: " + writeLockHeldTimeMsStat);
    LOG.info("Longest Write Lock Held Time (ms): " +
        omLockMetrics.getLongestWriteLockHeldTimeMs());
    int writeHeldSamples =
        Integer.parseInt(writeLockHeldTimeMsStat.split(" ")[2]);
    Assert.assertTrue("Write Lock Held Samples should be positive",
        writeHeldSamples > 0);
  }

  private static class ParameterBuilder {

    private String volumeName = "vol1";
    private String bucketName = "bucket1";
    private int keyCountForRead = 100;
    private int keyCountForWrite = 10;
    private long keySizeInBytes = 256;
    private int bufferSize = 64;
    private int length = 10;
    private int totalThreadCount = 100;
    private int readThreadPercentage = 90;
    private int numOfReadOperations = 50;
    private int numOfWriteOperations = 10;

    private ParameterBuilder setVolumeName(String volumeNameParam) {
      volumeName = volumeNameParam;
      return this;
    }

    private ParameterBuilder setBucketName(String bucketNameParam) {
      bucketName = bucketNameParam;
      return this;
    }

    private ParameterBuilder setKeyCountForRead(int keyCountForReadParam) {
      keyCountForRead = keyCountForReadParam;
      return this;
    }

    private ParameterBuilder setKeyCountForWrite(int keyCountForWriteParam) {
      keyCountForWrite = keyCountForWriteParam;
      return this;
    }

    private ParameterBuilder setKeySizeInBytes(long keySizeInBytesParam) {
      keySizeInBytes = keySizeInBytesParam;
      return this;
    }

    private ParameterBuilder setBufferSize(int bufferSizeParam) {
      bufferSize = bufferSizeParam;
      return this;
    }

    private ParameterBuilder setLength(int lengthParam) {
      length = lengthParam;
      return this;
    }

    private ParameterBuilder setTotalThreadCount(int totalThreadCountParam) {
      totalThreadCount = totalThreadCountParam;
      return this;
    }

    private ParameterBuilder setReadThreadPercentage(
        int readThreadPercentageParam) {
      readThreadPercentage = readThreadPercentageParam;
      return this;
    }

    private ParameterBuilder setNumOfReadOperations(
        int numOfReadOperationsParam) {
      numOfReadOperations = numOfReadOperationsParam;
      return this;
    }

    private ParameterBuilder setNumOfWriteOperations(
        int numOfWriteOperationsParam) {
      numOfWriteOperations = numOfWriteOperationsParam;
      return this;
    }
  }
}
