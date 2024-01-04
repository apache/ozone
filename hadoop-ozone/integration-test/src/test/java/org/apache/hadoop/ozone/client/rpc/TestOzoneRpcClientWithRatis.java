/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.om.ratis.metrics.OzoneManagerStateMachineMetrics;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class is to test all the public facing APIs of Ozone Client with an
 * active OM Ratis server.
 */
public class TestOzoneRpcClientWithRatis extends TestOzoneRpcClientAbstract {
  private static OzoneConfiguration conf;
  /**
   * Create a MiniOzoneCluster for testing.
   * Ozone is made active by setting OZONE_ENABLED = true.
   * Ozone OM Ratis server is made active by setting
   * OZONE_OM_RATIS_ENABLE = true;
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        false);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        true);
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    startCluster(conf);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() throws IOException {
    shutdownCluster();
  }

  /**
   * Tests get the information of key with network topology awareness enabled.
   * @throws IOException
   */
  @Test
  public void testGetKeyAndFileWithNetworkTopology() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    try (OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS, THREE), new HashMap<>())) {
      out.write(value.getBytes(UTF_8));
    }

    // Since the rpc client is outside of cluster, then getFirstNode should be
    // equal to getClosestNode.
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName);

    // read key with topology aware read enabled
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[value.getBytes(UTF_8).length];
      is.read(b);
      assertArrayEquals(b, value.getBytes(UTF_8));
    } catch (OzoneChecksumException e) {
      fail("Read key should succeed");
    }

    // read file with topology aware read enabled
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[value.getBytes(UTF_8).length];
      is.read(b);
      assertArrayEquals(b, value.getBytes(UTF_8));
    } catch (OzoneChecksumException e) {
      fail("Read file should succeed");
    }

    // read key with topology aware read disabled
    conf.setBoolean(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        false);
    try (OzoneClient newClient = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore newStore = newClient.getObjectStore();
      OzoneBucket newBucket =
          newStore.getVolume(volumeName).getBucket(bucketName);
      try (OzoneInputStream is = newBucket.readKey(keyName)) {
        byte[] b = new byte[value.getBytes(UTF_8).length];
        is.read(b);
        assertArrayEquals(b, value.getBytes(UTF_8));
      } catch (OzoneChecksumException e) {
        fail("Read key should succeed");
      }

      // read file with topology aware read disabled
      try (OzoneInputStream is = newBucket.readFile(keyName)) {
        byte[] b = new byte[value.getBytes(UTF_8).length];
        is.read(b);
        assertArrayEquals(b, value.getBytes(UTF_8));
      } catch (OzoneChecksumException e) {
        fail("Read file should succeed");
      }
    }
  }

  @Test
  public void testMultiPartUploadWithStream() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    byte[] sampleData = new byte[1024 * 8];

    int valueLength = sampleData.length;

    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    ReplicationConfig replicationConfig =
        ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS,
            THREE);

    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationConfig);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertNotNull(multipartInfo.getUploadID());

    OzoneDataStreamOutput ozoneStreamOutput = bucket.createMultipartStreamKey(
        keyName, valueLength, 1, uploadID);
    ozoneStreamOutput.write(ByteBuffer.wrap(sampleData), 0,
        valueLength);
    ozoneStreamOutput.close();

    OzoneMultipartUploadPartListParts parts =
        bucket.listParts(keyName, uploadID, 0, 1);

    assertEquals(parts.getPartInfoList().size(), 1);

    OzoneMultipartUploadPartListParts.PartInfo partInfo =
        parts.getPartInfoList().get(0);
    assertEquals(valueLength, partInfo.getSize());

  }

  @Test
  public void testUploadWithStreamAndMemoryMappedBuffer() throws IOException {
    // create a local dir
    final String dir = GenericTestUtils.getTempPath(
        getClass().getSimpleName());
    GenericTestUtils.assertDirCreation(new File(dir));

    // create a local file
    final int chunkSize = 1024;
    final byte[] data = new byte[8 * chunkSize];
    ThreadLocalRandom.current().nextBytes(data);
    final File file = new File(dir, "data");
    try (FileOutputStream out = new FileOutputStream(file)) {
      out.write(data);
    }

    // create a volume
    final String volumeName = "vol-" + UUID.randomUUID();
    getStore().createVolume(volumeName);
    final OzoneVolume volume = getStore().getVolume(volumeName);

    // create a bucket
    final String bucketName = "buck-" + UUID.randomUUID();
    final BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(ReplicationConfig.fromTypeAndFactor(
                ReplicationType.RATIS, THREE)))
        .build();
    volume.createBucket(bucketName, bucketArgs);
    final OzoneBucket bucket = volume.getBucket(bucketName);

    // upload a key from the local file using memory-mapped buffers
    final String keyName = "key-" + UUID.randomUUID();
    try (RandomAccessFile raf = new RandomAccessFile(file, "r");
         OzoneDataStreamOutput out = bucket.createStreamKey(
             keyName, data.length)) {
      final FileChannel channel = raf.getChannel();
      long off = 0;
      for (long len = raf.length(); len > 0;) {
        final long writeLen = Math.min(len, chunkSize);
        final ByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY,
            off, writeLen);
        out.write(mapped);
        off += writeLen;
        len -= writeLen;
      }
    }

    // verify the key details
    final OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    assertEquals(keyName, keyDetails.getName());
    assertEquals(data.length, keyDetails.getDataSize());

    // verify the key content
    final byte[] buffer = new byte[data.length];
    try (OzoneInputStream in = keyDetails.getContent()) {
      for (int off = 0; off < data.length;) {
        final int n = in.read(buffer, off, data.length - off);
        if (n < 0) {
          break;
        }
        off += n;
      }
    }
    assertArrayEquals(data, buffer);
  }

  @Test
  public void testParallelDeleteBucketAndCreateKey() throws IOException,
      InterruptedException, TimeoutException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    GenericTestUtils.LogCapturer omSMLog = GenericTestUtils.LogCapturer
        .captureLogs(OzoneManagerStateMachine.LOG);
    OzoneManagerStateMachine omSM = getCluster().getOzoneManager()
        .getOmRatisServer().getOmStateMachine();
    OzoneManagerStateMachineMetrics metrics = omSM.getMetrics();

    Thread thread1 = new Thread(() -> {
      try {
        volume.deleteBucket(bucketName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        getClient().getProxy().createKey(volumeName, bucketName, keyName,
            0, ReplicationType.RATIS, ONE, new HashMap<>());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    OMRequestHandlerPauseInjector injector =
        new OMRequestHandlerPauseInjector();
    omSM.getHandler().setInjector(injector);
    thread1.start();
    thread2.start();
    GenericTestUtils.waitFor(() -> metrics.getApplyTransactionMapSize() > 0,
        100, 5000);
    Thread.sleep(2000);
    injector.resume();

    try {
      thread1.join();
      thread2.join();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    omSM.getHandler().setInjector(null);
    // Generate more write requests to OM
    String newBucketName = UUID.randomUUID().toString();
    volume.createBucket(newBucketName);
    OzoneBucket bucket = volume.getBucket(newBucketName);
    for (int i = 0; i < 10; i++) {
      bucket.createKey("key-" + i, value.getBytes(UTF_8).length,
          ReplicationType.RATIS, ONE, new HashMap<>());
    }

    Assert.assertTrue(
        omSMLog.getOutput().contains("Failed to write, Exception occurred"));
    GenericTestUtils.waitFor(() -> metrics.getApplyTransactionMapSize() == 0,
        100, 5000);
  }

  private static class OMRequestHandlerPauseInjector extends FaultInjector {
    private CountDownLatch ready;
    private CountDownLatch wait;

    OMRequestHandlerPauseInjector() {
      init();
    }

    @Override
    public void init() {
      this.ready = new CountDownLatch(1);
      this.wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void resume() throws IOException {
      // Make sure injector pauses before resuming.
      try {
        ready.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        assertTrue(fail("resume interrupted"));
      }
      wait.countDown();
    }

    @Override
    public void reset() throws IOException {
      init();
    }
  }
}
