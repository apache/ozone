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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verify BlockOutputStream with incremental PutBlock feature.
 * (ozone.client.incremental.chunk.list = true)
 */
public class TestBlockOutputStreamIncrementalPutBlock {
  private OzoneClient client;
  private final String keyName = UUID.randomUUID().toString();
  private final String volumeName = UUID.randomUUID().toString();
  private final String bucketName = UUID.randomUUID().toString();
  private OzoneBucket bucket;
  private final ConfigurationSource config = new InMemoryConfiguration();

  public static Iterable<Boolean> parameters() {
    return Arrays.asList(true, false);
  }

  private void init(boolean incrementalChunkList) throws IOException {
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);

    clientConfig.setIncrementalChunkList(incrementalChunkList);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.CRC32C);

    ((InMemoryConfiguration)config).setFromObject(clientConfig);

    ((InMemoryConfiguration) config).setBoolean(
        OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    ((InMemoryConfiguration) config).setBoolean(
        "ozone.client.hbase.enhancements.allowed", true);
    ((InMemoryConfiguration) config).setBoolean(
        OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    ((InMemoryConfiguration) config).setInt(
        "ozone.client.bytes.per.checksum", 8192);

    RpcClient rpcClient = new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(
          String omServiceId)
          throws IOException {
        return new MockOmTransport();
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) throws IOException {
        return new MockXceiverClientFactory();
      }
    };

    client = new OzoneClient(config, rpcClient);
    ObjectStore store = client.getObjectStore();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
  }

  @AfterEach
  public void close() throws IOException {
    client.close();
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void writeSmallChunk(boolean incrementalChunkList)
      throws IOException {
    init(incrementalChunkList);

    int size = 1024;
    String s = RandomStringUtils.secure().nextAlphabetic(1024);
    ByteBuffer byteBuffer = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));

    try (OzoneOutputStream out = bucket.createKey(keyName, size,
        ReplicationConfig.getDefault(config), new HashMap<>())) {
      for (int i = 0; i < 4097; i++) {
        out.write(byteBuffer);
        out.hsync();
      }
    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      ByteBuffer readBuffer = ByteBuffer.allocate(size);
      for (int i = 0; i < 4097; i++) {
        is.read(readBuffer);
        assertArrayEquals(readBuffer.array(), byteBuffer.array());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void writeLargeChunk(boolean incrementalChunkList)
      throws IOException {
    init(incrementalChunkList);

    int size = 1024 * 1024 + 1;
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);

    try (OzoneOutputStream out = bucket.createKey(keyName, size,
        ReplicationConfig.getDefault(config), new HashMap<>())) {
      for (int i = 0; i < 4; i++) {
        out.write(byteBuffer);
        out.hsync();
      }
    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      ByteBuffer readBuffer = ByteBuffer.allocate(size);
      for (int i = 0; i < 4; i++) {
        is.read(readBuffer);
        assertArrayEquals(readBuffer.array(), byteBuffer.array());
      }
    }
  }
}
