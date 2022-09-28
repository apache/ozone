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
import org.apache.ozone.test.tag.Flaky;
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
  // For Freon
  private static String path;

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

    // Hack to get freon working
//    path = GenericTestUtils
//        .getTempPath(TestHadoopDirTreeGenerator.class.getSimpleName());
//    FileOutputStream out = FileUtils.openOutputStream(new File(path, "conf"));
//    cluster.getConf().writeXml(out);
//    out.getFD().sync();
//    out.close();

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
  void testDefault() {
    RandomKeyGenerator randomKeyGenerator =
        new RandomKeyGenerator(cluster.getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);
    cmd.execute("--num-of-volumes", "2",
        "--num-of-buckets", "5",
        "--num-of-keys", "10");

    Assert.assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
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
        "--key-size", "10240",
        "--factor", "THREE",
        "--type", "RATIS"
    );

    Assert.assertEquals(10, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  void testZeroSizeKey() throws IOException {

//    cluster.getOzoneManager().getMetadataManager().getStore().compactDB();

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

//    Assert.assertEquals(1, randomKeyGenerator.getNumberOfVolumesCreated());
//    Assert.assertEquals(1, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(6000L, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(6000L,
        randomKeyGenerator.getSuccessfulValidationCount());

//    cluster.getOzoneManager().getMetadataManager().getStore().flushDB();
//    cluster.getOzoneManager().getMetadataManager().getStore().compactDB();

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

//    cluster.getOzoneManager().getMetadataManager().getStore().flushDB();
//    cluster.getOzoneManager().getMetadataManager().getStore().compactDB();

    // Create snapshot
    String resp = store.createSnapshot(volumeName, bucketName, "snap1");
    System.out.println(resp);

//    cluster.getOzoneManager().getMetadataManager().getStore().flushDB();
//    cluster.getOzoneManager().getMetadataManager().getStore().compactDB();

//    cmd.execute("--num-of-volumes", "1",
//        "--num-of-buckets", "1",
//        "--num-of-keys", "1000",
//        "--num-of-threads", "1",
//        "--key-size", "0",
//        "--factor", "THREE",
//        "--type", "RATIS",
//        "--validate-writes"
//    );

//    verifyFreonCommand(new ParameterBuilder()
//        .setVolumeName(volumeName).setBucketName(bucketName)
//        .setTotalThreadCount(1)
//        .setNumOfReadOperations(1).setNumOfWriteOperations(1)
//        .setKeyCountForRead(100).setKeyCountForWrite(5000));

    final OzoneVolume volume = store.getVolume(volumeName);
    final OzoneBucket bucket = volume.getBucket(bucketName);

//    for (int i = 0; i < 1000; i++) {
//      bucket.createKey("a_" + i, 0).close();
//    }
//    resp = store.createSnapshot(volumeName, bucketName, "snap2");
//    System.out.println(resp);

    for (int i = 0; i < 6000; i++) {
      bucket.createKey("b_" + i, 0).close();
    }

//    cluster.getOzoneManager().getMetadataManager().getStore().flushDB();
//    cluster.getOzoneManager().getMetadataManager().getStore().compactDB();
    resp = store.createSnapshot(volumeName, bucketName, "snap3");

//    cluster.getOzoneManager().getMetadataManager().getStore().flushDB();
//    cluster.getOzoneManager().getMetadataManager().getStore().compactDB();
    System.out.println(resp);
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

    Assert.assertEquals(10, randomKeyGenerator.getThreadPoolSize());
    Assert.assertEquals(1, randomKeyGenerator.getNumberOfKeysAdded());
  }

  @Test
  @Flaky("HDDS-5993")
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

    Assert.assertEquals(2, randomKeyGenerator.getNumberOfVolumesCreated());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCreated());
    Assert.assertEquals(100, randomKeyGenerator.getNumberOfKeysAdded());
    Assert.assertEquals(2, randomKeyGenerator.getNumberOfVolumesCleaned());
    Assert.assertEquals(10, randomKeyGenerator.getNumberOfBucketsCleaned());
  }

//  private void verifyFreonCommand(ParameterBuilder parameterBuilder)
//      throws IOException {
////    store.createVolume(parameterBuilder.volumeName);
//    OzoneVolume volume = store.getVolume(parameterBuilder.volumeName);
////    volume.createBucket(parameterBuilder.bucketName);
//    OzoneBucket bucket = volume.getBucket(parameterBuilder.bucketName);
//    String confPath = new File(path, "conf").getAbsolutePath();
//
//    long startTime = System.currentTimeMillis();
//    new Freon().execute(
//        new String[]{"-conf", confPath, "obrwk",
//            "-v", parameterBuilder.volumeName,
//            "-b", parameterBuilder.bucketName,
//            "-k", String.valueOf(parameterBuilder.keyCountForRead),
//            "-w", String.valueOf(parameterBuilder.keyCountForWrite),
//            "-g", String.valueOf(parameterBuilder.keySizeInBytes),
//            "--buffer", String.valueOf(parameterBuilder.bufferSize),
//            "-l", String.valueOf(parameterBuilder.length),
//            "-c", String.valueOf(parameterBuilder.totalThreadCount),
//            "-T", String.valueOf(parameterBuilder.readThreadPercentage),
//            "-R", String.valueOf(parameterBuilder.numOfReadOperations),
//            "-W", String.valueOf(parameterBuilder.numOfWriteOperations),
//            "-n", String.valueOf(1)});
//    long totalTime = System.currentTimeMillis() - startTime;
//    LOG.info("Total Execution Time: " + totalTime);
//
//    LOG.info("Started verifying OM bucket read/write ops key generation...");
//    verifyKeyCreation(parameterBuilder.keyCountForRead, bucket, "/readPath/");
//
//    int readThreadCount = (parameterBuilder.readThreadPercentage *
//        parameterBuilder.totalThreadCount) / 100;
//    int writeThreadCount = parameterBuilder.totalThreadCount - readThreadCount;
//    verifyKeyCreation(writeThreadCount * parameterBuilder.keyCountForWrite *
//        parameterBuilder.numOfWriteOperations, bucket, "/writePath/");
//
//    verifyOMLockMetrics(cluster.getOzoneManager().getMetadataManager().getLock()
//        .getOMLockMetrics());
//  }
//
//  private void verifyKeyCreation(int expectedCount, OzoneBucket bucket,
//      String keyPrefix) throws IOException {
//    int actual = 0;
//    Iterator<? extends OzoneKey> ozoneKeyIterator = bucket.listKeys(keyPrefix);
//    while (ozoneKeyIterator.hasNext()) {
//      ozoneKeyIterator.next();
//      ++actual;
//    }
//    Assert.assertEquals("Mismatch Count!", expectedCount, actual);
//  }
//
//  private void verifyOMLockMetrics(OMLockMetrics omLockMetrics) {
//    String readLockWaitingTimeMsStat =
//        omLockMetrics.getReadLockWaitingTimeMsStat();
//    LOG.info("Read Lock Waiting Time Stat: " + readLockWaitingTimeMsStat);
//    LOG.info("Longest Read Lock Waiting Time (ms): " +
//        omLockMetrics.getLongestReadLockWaitingTimeMs());
//    int readWaitingSamples =
//        Integer.parseInt(readLockWaitingTimeMsStat.split(" ")[2]);
//    Assert.assertTrue("Read Lock Waiting Samples should be positive",
//        readWaitingSamples > 0);
//
//    String readLockHeldTimeMsStat = omLockMetrics.getReadLockHeldTimeMsStat();
//    LOG.info("Read Lock Held Time Stat: " + readLockHeldTimeMsStat);
//    LOG.info("Longest Read Lock Held Time (ms): " +
//        omLockMetrics.getLongestReadLockHeldTimeMs());
//    int readHeldSamples =
//        Integer.parseInt(readLockHeldTimeMsStat.split(" ")[2]);
//    Assert.assertTrue("Read Lock Held Samples should be positive",
//        readHeldSamples > 0);
//
//    String writeLockWaitingTimeMsStat =
//        omLockMetrics.getWriteLockWaitingTimeMsStat();
//    LOG.info("Write Lock Waiting Time Stat: " + writeLockWaitingTimeMsStat);
//    LOG.info("Longest Write Lock Waiting Time (ms): " +
//        omLockMetrics.getLongestWriteLockWaitingTimeMs());
//    int writeWaitingSamples =
//        Integer.parseInt(writeLockWaitingTimeMsStat.split(" ")[2]);
//    Assert.assertTrue("Write Lock Waiting Samples should be positive",
//        writeWaitingSamples > 0);
//
//    String writeLockHeldTimeMsStat = omLockMetrics.getWriteLockHeldTimeMsStat();
//    LOG.info("Write Lock Held Time Stat: " + writeLockHeldTimeMsStat);
//    LOG.info("Longest Write Lock Held Time (ms): " +
//        omLockMetrics.getLongestWriteLockHeldTimeMs());
//    int writeHeldSamples =
//        Integer.parseInt(writeLockHeldTimeMsStat.split(" ")[2]);
//    Assert.assertTrue("Write Lock Held Samples should be positive",
//        writeHeldSamples > 0);
//  }
//
//  private static class ParameterBuilder {
//
//    private String volumeName = "vol1";
//    private String bucketName = "bucket1";
//    private int keyCountForRead = 100;
//    private int keyCountForWrite = 10;
//    private long keySizeInBytes = 256;
//    private int bufferSize = 64;
//    private int length = 10;
//    private int totalThreadCount = 100;
//    private int readThreadPercentage = 90;
//    private int numOfReadOperations = 50;
//    private int numOfWriteOperations = 10;
//
//    private ParameterBuilder setVolumeName(String volumeNameParam) {
//      volumeName = volumeNameParam;
//      return this;
//    }
//
//    private ParameterBuilder setBucketName(String bucketNameParam) {
//      bucketName = bucketNameParam;
//      return this;
//    }
//
//    private ParameterBuilder setKeyCountForRead(int keyCountForReadParam) {
//      keyCountForRead = keyCountForReadParam;
//      return this;
//    }
//
//    private ParameterBuilder setKeyCountForWrite(int keyCountForWriteParam) {
//      keyCountForWrite = keyCountForWriteParam;
//      return this;
//    }
//
//    private ParameterBuilder setKeySizeInBytes(long keySizeInBytesParam) {
//      keySizeInBytes = keySizeInBytesParam;
//      return this;
//    }
//
//    private ParameterBuilder setBufferSize(int bufferSizeParam) {
//      bufferSize = bufferSizeParam;
//      return this;
//    }
//
//    private ParameterBuilder setLength(int lengthParam) {
//      length = lengthParam;
//      return this;
//    }
//
//    private ParameterBuilder setTotalThreadCount(int totalThreadCountParam) {
//      totalThreadCount = totalThreadCountParam;
//      return this;
//    }
//
//    private ParameterBuilder setReadThreadPercentage(
//        int readThreadPercentageParam) {
//      readThreadPercentage = readThreadPercentageParam;
//      return this;
//    }
//
//    private ParameterBuilder setNumOfReadOperations(
//        int numOfReadOperationsParam) {
//      numOfReadOperations = numOfReadOperationsParam;
//      return this;
//    }
//
//    private ParameterBuilder setNumOfWriteOperations(
//        int numOfWriteOperationsParam) {
//      numOfWriteOperations = numOfWriteOperationsParam;
//      return this;
//    }
//  }
}
