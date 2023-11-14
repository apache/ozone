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

package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;

import static org.junit.Assert.assertArrayEquals;

/**
 * Verify BlockOutputStream with incremental PutBlock feature.
 * (incremental.chunk.list = true)
 */
@RunWith(Parameterized.class)
public class TestBlockOutputStreamIncrementalPutBlock {
  private OzoneClient client;
  private ObjectStore store;
  private RpcClient rpcClient;

  private String keyName = UUID.randomUUID().toString();
  private String volumeName = UUID.randomUUID().toString();
  private String bucketName = UUID.randomUUID().toString();
  private ConfigurationSource config = new InMemoryConfiguration();
  private boolean enableIncrementalChunkList;


  @Parameterized.Parameters
  public static Iterable<Boolean> parameters() {
    return Arrays.asList(true, false);
  }

  public TestBlockOutputStreamIncrementalPutBlock(
      Boolean enableIncrementalChunkList) {
    this.enableIncrementalChunkList = enableIncrementalChunkList;
  }

  @Before
  public void init() throws IOException {
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);

    clientConfig.setIncrementalChunkList(this.enableIncrementalChunkList);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.CRC32C);

    ((InMemoryConfiguration)config).setFromObject(clientConfig);

    rpcClient = new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(
          String omServiceId)
          throws IOException {
        return new MockOmTransport();
      }

      @NotNull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          List<X509Certificate> x509Certificates)
          throws IOException {
        return new MockXceiverClientFactory();
      }
    };

    client = new OzoneClient(config, rpcClient);
    store = client.getObjectStore();
  }

  @After
  public void close() throws IOException {
    client.close();
  }

  @Test
  public void writeSmallKey() throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    int size = 1024;
    String s = RandomStringUtils.randomAlphabetic(1024);
    ByteBuffer byteBuffer = ByteBuffer.wrap(s.getBytes());

    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, size,
        ReplicationConfig.getDefault(config), new HashMap<>())) {
      for (int i = 0; i < 2; i++) {
        out.write(byteBuffer);
        out.hsync();
      }
    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      ByteBuffer readBuffer = ByteBuffer.allocate(size);
      for (int i = 0; i< 2; i++) {
        is.read(readBuffer);
        assertArrayEquals(readBuffer.array(), byteBuffer.array());
      }
    }
  }

  @Test
  public void writeLargeKey() throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    int size = 1024 * 1024 + 1;
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);

    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    try (OzoneOutputStream out = bucket.createKey(keyName, size,
        ReplicationConfig.getDefault(config), new HashMap<>())) {
      out.write(byteBuffer);
      out.hsync();
    }

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      ByteBuffer readBuffer = ByteBuffer.allocate(size);
      is.read(readBuffer);
      assertArrayEquals(readBuffer.array(), byteBuffer.array());
    }
  }
}
