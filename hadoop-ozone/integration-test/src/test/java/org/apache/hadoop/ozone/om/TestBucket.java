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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;

/**
 * Wrapper for {@code OzoneBucket} for testing.  Can create random keys,
 * verify content, etc.
 */
public final class TestBucket {

  private final OzoneBucket bucket;

  private TestBucket(OzoneBucket bucket) {
    this.bucket = bucket;
  }

  public static Builder newBuilder(OzoneClient client) {
    return new Builder(client);
  }

  public OzoneBucket delegate() {
    return bucket;
  }

  public KeyInputStream getKeyInputStream(String keyName) throws IOException {
    return (KeyInputStream) bucket
        .readKey(keyName).getInputStream();
  }

  public byte[] writeKey(String keyName, int dataLength) throws Exception {
    ReplicationConfig repConfig = RatisReplicationConfig.getInstance(THREE);
    return writeKey(keyName, repConfig, dataLength);
  }

  public byte[] writeKey(String key, ReplicationConfig repConfig, int len)
      throws Exception {
    byte[] inputData = ContainerTestHelper
        .getFixedLengthString(UUID.randomUUID().toString(), len)
        .getBytes(UTF_8);

    writeKey(key, repConfig, inputData);

    return inputData;
  }

  public void writeKey(String key, ReplicationConfig repConfig,
      byte[] inputData) throws IOException {
    TestDataUtil.createKey(bucket, key, repConfig, inputData);
  }

  public byte[] writeRandomBytes(String keyName, int dataLength)
      throws Exception {
    ReplicationConfig repConfig = RatisReplicationConfig.getInstance(THREE);
    return writeRandomBytes(keyName, repConfig, dataLength);
  }

  public byte[] writeRandomBytes(String key, ReplicationConfig repConfig,
      int len) throws Exception {
    byte[] inputData = new byte[len];
    ThreadLocalRandom.current().nextBytes(inputData);

    writeKey(key, repConfig, inputData);
    return inputData;
  }

  public void validateData(byte[] inputData, int offset, byte[] readData) {
    int readDataLen = readData.length;
    byte[] expectedData = new byte[readDataLen];
    System.arraycopy(inputData, offset, expectedData, 0, readDataLen);

    assertArrayEquals(expectedData, readData);
  }

  /**
   * Builder for {@code TestBucket}.
   */
  public static class Builder {
    private final OzoneClient client;
    private OzoneVolume volume;
    private String volumeName;
    private String bucketName;

    Builder(OzoneClient client) {
      this.client = client;
    }

    public TestBucket build() throws IOException {
      ObjectStore objectStore = client.getObjectStore();
      if (volume == null) { // TODO add setVolume
        if (volumeName == null) { // TODO add setVolumeName
          volumeName = "vol" + RandomStringUtils.secure().nextNumeric(10);
        }
        objectStore.createVolume(volumeName);
        volume = objectStore.getVolume(volumeName);
      }
      if (bucketName == null) { // TODO add setBucketName
        bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(10);
      }
      volume.createBucket(bucketName);
      return new TestBucket(volume.getBucket(bucketName));
    }
  }
}
