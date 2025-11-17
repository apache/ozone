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

package org.apache.hadoop.ozone.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.ozone.test.GenericTestUtils.getTestStartTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
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
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.ozone.test.LambdaTestUtils.VoidCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    OMException ex = assertThrows(OMException.class, () -> eval.call());
    assertEquals(code, ex.getResult());
  }

  @BeforeEach
  public void init() throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    createNewClient(config, new SinglePipelineBlockAllocator(config));
  }

  private void createNewClient(ConfigurationSource config,
      MockBlockAllocator blkAllocator) throws IOException {
    client = new OzoneClient(config, new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        return new MockOmTransport(blkAllocator);
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    });

    store = client.getObjectStore();
  }

  @AfterEach
  public void close() throws IOException {
    client.close();
  }

  @Test
  public void testDeleteVolume()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    assertNotNull(volume);
    store.deleteVolume(volumeName);
    expectOmException(ResultCodes.VOLUME_NOT_FOUND,
        () -> store.getVolume(volumeName));

  }

  @Test
  public void testCreateVolumeWithMetadata()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .build();
    store.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = store.getVolume(volumeName);
    assertEquals(OzoneConsts.QUOTA_RESET,
        volume.getQuotaInNamespace());
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());
    assertEquals("val1", volume.getMetadata().get("key1"));
    assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateBucket()
      throws IOException {
    Instant testStartTime = getTestStartTime();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    assertFalse(volume.getCreationTime().isBefore(testStartTime));
  }

  @Test
  public void testPutKeyRatisOneNode() throws IOException {
    Instant testStartTime = getTestStartTime();
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
      assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      assertEquals(value.length(), is.read(fileContent));
      is.close();
      assertEquals(value, new String(fileContent, UTF_8));
      assertFalse(key.getCreationTime().isBefore(testStartTime));
      assertFalse(key.getModificationTime().isBefore(testStartTime));
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
    ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);
    validator.disableValidation();
    config.setFromObject(validator);
    config.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 2,
        StorageUnit.KB);
    int data = 3;
    int parity = 2;
    int chunkSize = 1024;
    createNewClient(config,
        new MultiNodePipelineBlockAllocator(config, data + parity, 15));
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
      assertEquals(keyName, key.getName());
    }
  }

  /**
   * This test validates that for S3G,
   * the key upload process needs to be atomic.
   * It simulates two mismatch scenarios where the actual write data size does
   * not match the expected size.
   */
  @Test
  public void testPutKeySizeMismatch() throws IOException {
    String value = new String(new byte[1024], UTF_8);
    OzoneBucket bucket = getOzoneBucket();
    String keyName = UUID.randomUUID().toString();
    try {
      // Simulating first mismatch: Write less data than expected
      client.getProxy().setIsS3Request(true);
      OzoneOutputStream out1 = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS, ONE,
          new HashMap<>());
      out1.write(value.substring(0, value.length() - 1).getBytes(UTF_8));
      assertThrows(IllegalStateException.class, out1::close,
          "Expected IllegalArgumentException due to size mismatch.");

      // Simulating second mismatch: Write more data than expected
      OzoneOutputStream out2 = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS, ONE,
          new HashMap<>());
      value += "1";
      out2.write(value.getBytes(UTF_8));
      assertThrows(IllegalStateException.class, out2::close,
          "Expected IllegalArgumentException due to size mismatch.");
    } finally {
      client.getProxy().setIsS3Request(false);
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
