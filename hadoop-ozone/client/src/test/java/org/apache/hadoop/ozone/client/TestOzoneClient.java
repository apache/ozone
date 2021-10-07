/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.ozone.test.LambdaTestUtils.VoidCallable;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;

/**
 * Real unit test for OzoneClient.
 * <p>
 * Used for testing Ozone client without external network calls.
 */
public class TestOzoneClient {

  private OzoneClient client;
  private ObjectStore store;

  public static <E extends Throwable> void expectOmException(
      OMException.ResultCodes code,
      VoidCallable eval)
      throws Exception {
    try {
      eval.call();
      Assert.fail("OMException is expected");
    } catch (OMException ex) {
      Assert.assertEquals(code, ex.getResult());
    }
  }

  @Before
  public void init() throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    createNewClient(config, new SinglePipelineBlockAllocator(config));
  }

  private void createNewClient(ConfigurationSource config,
      MockBlockAllocator blkAllocator) throws IOException {
    client = new OzoneClient(config, new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(String omServiceId)
          throws IOException {
        return new MockOmTransport(blkAllocator);
      }

      @NotNull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          List<X509Certificate> x509Certificates) throws IOException {
        return new MockXceiverClientFactory();
      }
    });

    store = client.getObjectStore();
  }

  @After
  public void close() throws IOException {
    client.close();
  }

  @Test
  public void testDeleteVolume()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertNotNull(volume);
    store.deleteVolume(volumeName);
    expectOmException(ResultCodes.VOLUME_NOT_FOUND,
        () -> store.getVolume(volumeName));

  }

  @Test
  public void testCreateVolumeWithMetadata()
      throws IOException, OzoneClientException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .build();
    store.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInNamespace());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());
    Assert.assertEquals("val1", volume.getMetadata().get("key1"));
    Assert.assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateBucket()
      throws IOException {
    Instant testStartTime = Instant.now();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    Assert.assertFalse(volume.getCreationTime().isBefore(testStartTime));
  }

  @Test
  public void testPutKeyRatisOneNode() throws IOException {
    Instant testStartTime = Instant.now();
    String value = "sample value";
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS,
          ONE, new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      Assert.assertEquals(value.length(), is.read(fileContent));
      is.close();
      Assert.assertEquals(value, new String(fileContent, UTF_8));
      Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
      Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
  }

  @Test
  public void testPutKeyAllocateBlock() throws IOException {
    String value = new String(new byte[1024], UTF_8);
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      try (OzoneOutputStream out = bucket
          .createKey(keyName, value.getBytes(UTF_8).length,
              ReplicationType.RATIS, ONE, new HashMap<>())) {
        out.write(value.getBytes(UTF_8));
        out.write(value.getBytes(UTF_8));
      }
    }
  }

  @Test
  public void testPutKeyWithECReplicationConfig() throws IOException {
    close();
    OzoneConfiguration config = new OzoneConfiguration();
    config.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 2,
        StorageUnit.KB);
    int data = 3;
    int parity = 2;
    int chunkSize = 1024;
    createNewClient(config, new MultiNodePipelineBlockAllocator(
        config, data + parity));
    String value = new String(new byte[chunkSize], UTF_8);
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();
      try (OzoneOutputStream out = bucket
          .createKey(keyName, value.getBytes(UTF_8).length,
              new ECReplicationConfig(data, parity,
                  ECReplicationConfig.EcCodec.RS, chunkSize),
              new HashMap<>())) {
        out.write(value.getBytes(UTF_8));
        out.write(value.getBytes(UTF_8));
      }
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
    }
  }

  private OzoneBucket getOzoneBucket() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    return volume.getBucket(bucketName);
  }
}