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
package org.apache.hadoop.ozone.client.rpc.read;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Common tests for Ozone's {@code InputStream} implementations.
 */
@RunWith(Parameterized.class)
public abstract class TestInputStreamBase {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private ObjectStore objectStore;

  private String volumeName;
  private String bucketName;
  private String keyString;

  private ContainerLayoutVersion containerLayout;
  private static final Random RAND = new Random();

  protected static final int CHUNK_SIZE = 1024 * 1024;          // 1MB
  protected static final int FLUSH_SIZE = 2 * CHUNK_SIZE;       // 2MB
  protected static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;   // 4MB
  protected static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;   // 8MB
  protected static final int BYTES_PER_CHECKSUM = 256 * 1024;   // 256KB

  @Rule
  public Timeout timeout = Timeout.seconds(300);

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  public TestInputStreamBase(ContainerLayoutVersion layout) {
    this.containerLayout = layout;
  }

  /**
   * Create a MiniDFSCluster for testing.
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    OzoneClientConfig config = new OzoneClientConfig();
    config.setBytesPerChecksum(BYTES_PER_CHECKSUM);
    conf.setFromObject(config);

    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 64,
        StorageUnit.MB);
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY,
        containerLayout.toString());

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .setTotalPipelineNumLimit(5)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyString = UUID.randomUUID().toString();

    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  MiniOzoneCluster getCluster() {
    return cluster;
  }

  String getVolumeName() {
    return volumeName;
  }

  String getBucketName() {
    return bucketName;
  }

  KeyInputStream getKeyInputStream(String keyName) throws IOException {
    return (KeyInputStream) objectStore
        .getVolume(volumeName)
        .getBucket(bucketName)
        .readKey(keyName).getInputStream();
  }

  String getNewKeyName() {
    return UUID.randomUUID().toString();
  }

  byte[] writeKey(String keyName, int dataLength) throws Exception {
    ReplicationConfig repConfig = RatisReplicationConfig.getInstance(THREE);
    return writeKey(keyName, repConfig, dataLength);
  }

  byte[] writeKey(String keyName, ReplicationConfig repConfig, int dataLength)
      throws Exception {
    OzoneOutputStream key = TestHelper.createKey(keyName,
        repConfig, 0, objectStore, volumeName, bucketName);

    byte[] inputData = ContainerTestHelper.getFixedLengthString(
        keyString, dataLength).getBytes(UTF_8);
    key.write(inputData);
    key.close();

    return inputData;
  }

  byte[] writeRandomBytes(String keyName, int dataLength)
      throws Exception {
    ReplicationConfig repConfig = RatisReplicationConfig.getInstance(THREE);
    return writeRandomBytes(keyName, repConfig, dataLength);
  }

  byte[] writeRandomBytes(String keyName, ReplicationConfig repConfig,
      int dataLength)
      throws Exception {
    OzoneOutputStream key = TestHelper.createKey(keyName,
        repConfig, 0, objectStore, volumeName, bucketName);

    byte[] inputData = new byte[dataLength];
    RAND.nextBytes(inputData);
    key.write(inputData);
    key.close();

    return inputData;
  }

  void validateData(byte[] inputData, int offset, byte[] readData) {
    int readDataLen = readData.length;
    byte[] expectedData = new byte[readDataLen];
    System.arraycopy(inputData, (int) offset, expectedData, 0, readDataLen);

    for (int i = 0; i < readDataLen; i++) {
      Assert.assertEquals("Read data at does not match the input data at " +
              "position " + (offset + i), expectedData[i], readData[i]);
    }
  }
}
