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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.apache.hadoop.hdds.utils.IOUtils.closeQuietly;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test OzoneFSInputStream by reading through multiple interfaces.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneFSInputStream implements NonHATests.TestCase {

  private OzoneClient client;
  private FileSystem fs;
  private FileSystem ecFs;
  private Path filePath = null;
  private byte[] data = null;

  @BeforeAll
  void init() throws Exception {
    client = cluster().newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    // Set the fs.defaultFS and start the filesystem
    String uri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    fs =  FileSystem.get(URI.create(uri), cluster().getConf());
    int fileLen = 30 * 1024 * 1024;
    data = string2Bytes(RandomStringUtils.secure().nextAlphanumeric(fileLen));
    filePath = new Path("/" + RandomStringUtils.secure().nextAlphanumeric(5));
    try (FSDataOutputStream stream = fs.create(filePath)) {
      stream.write(data);
    }

    // create EC bucket to be used by OzoneFileSystem
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    builder.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
                (int) OzoneConsts.MB)));
    BucketArgs omBucketArgs = builder.build();
    String ecBucket = UUID.randomUUID().toString();
    TestDataUtil.createBucket(client, bucket.getVolumeName(), omBucketArgs,
        ecBucket);
    String ecUri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, ecBucket, bucket.getVolumeName());
    ecFs =  FileSystem.get(URI.create(ecUri), cluster().getConf());
  }

  @AfterAll
  void shutdown() {
    closeQuietly(client, fs, ecFs);
  }

  @Test
  public void testO3FSSingleByteRead() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      byte[] value = new byte[data.length];
      int i = 0;
      while (true) {
        int val = inputStream.read();
        if (val == -1) {
          break;
        }
        value[i] = (byte) val;
        assertEquals(value[i], data[i], "value mismatch at:" + i);
        i++;
      }
      assertEquals(i, data.length);
      assertArrayEquals(value, data);
    }
  }

  @Test
  public void testByteBufferPositionedRead() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      int bufferCapacity = 20;
      ByteBuffer buffer = ByteBuffer.allocate(bufferCapacity);
      long currentPos = inputStream.getPos();
      // Read positional data from 50th index
      int position = 50;
      int readBytes = inputStream.read(position, buffer);

      // File position should not be changed after positional read
      assertEquals(currentPos, inputStream.getPos());
      // Total read bytes should be equal to bufferCapacity
      // As file has more data than bufferCapacity
      assertEquals(readBytes, bufferCapacity);
      byte[] value1 = new byte[readBytes];
      System.arraycopy(buffer.array(), 0, value1, 0, readBytes);
      byte[] value2 = new byte[readBytes];
      System.arraycopy(data, position, value2, 0, readBytes);
      // Verify input and positional read data
      assertArrayEquals(value1, value2, "value mismatch");
      buffer.clear();

      // Read positional from 8th index again using same inputStream
      position = 8;
      readBytes = inputStream.read(position, buffer);
      assertEquals(currentPos, inputStream.getPos());
      assertEquals(readBytes, bufferCapacity);
      byte[] value3 = new byte[readBytes];
      System.arraycopy(buffer.array(), 0, value3, 0, readBytes);
      byte[] value4 = new byte[readBytes];
      System.arraycopy(data, position, value4, 0, readBytes);
      // Verify input and positional read data
      assertArrayEquals(value3, value4, "value mismatch");

      // Buffer size more than actual data, still read should succeed
      ByteBuffer buffer1 = ByteBuffer.allocate(30 * 1024 * 1024 * 2);
      // Read positional from 12th index
      position = 12;
      readBytes = inputStream.read(position, buffer1);
      assertEquals(currentPos, inputStream.getPos());
      // Total read bytes should be (total file bytes - position) as buffer is not filled completely
      assertEquals(readBytes, 30 * 1024 * 1024 - position);

      byte[] value5 = new byte[readBytes];
      System.arraycopy(buffer1.array(), 0, value5, 0, readBytes);
      byte[] value6 = new byte[readBytes];
      System.arraycopy(data, position, value6, 0, readBytes);
      // Verify input and positional read data
      assertArrayEquals(value5, value6, "value mismatch");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, 30 * 1024 * 1024, 30 * 1024 * 1024 + 1 })
  public void testByteBufferPositionedReadWithInvalidPosition(int position) throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      long currentPos = inputStream.getPos();
      ByteBuffer buffer = ByteBuffer.allocate(20);
      assertEquals(-1, inputStream.read(position, buffer));
      // File position should not be changed
      assertEquals(currentPos, inputStream.getPos());
    }
  }

  @Test
  public void testByteBufferPositionedReadFully() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      int bufferCapacity = 20;
      long currentPos = inputStream.getPos();
      ByteBuffer buffer = ByteBuffer.allocate(bufferCapacity);
      // Read positional data from 50th index
      int position = 50;
      inputStream.readFully(position, buffer);
      // File position should not be changed after positional readFully
      assertEquals(currentPos, inputStream.getPos());
      // Make sure buffer is full after readFully
      assertFalse(buffer.hasRemaining());

      byte[] value1 = new byte[bufferCapacity];
      System.arraycopy(buffer.array(), 0, value1, 0, bufferCapacity);
      byte[] value2 = new byte[bufferCapacity];
      System.arraycopy(data, position, value2, 0, bufferCapacity);
      // Verify input and positional read data
      assertArrayEquals(value1, value2, "value mismatch");
      buffer.clear();

      // Read positional from 8th index again using same inputStream
      position = 8;
      inputStream.readFully(position, buffer);
      assertEquals(currentPos, inputStream.getPos());
      assertFalse(buffer.hasRemaining());
      byte[] value3 = new byte[bufferCapacity];
      System.arraycopy(buffer.array(), 0, value3, 0, bufferCapacity);
      byte[] value4 = new byte[bufferCapacity];
      System.arraycopy(data, position, value4, 0, bufferCapacity);
      // Verify input and positional read data
      assertArrayEquals(value3, value4, "value mismatch");

      // Buffer size is more than actual data, readFully should fail in this case
      ByteBuffer buffer1 = ByteBuffer.allocate(30 * 1024 * 1024 * 2);
      assertThrows(EOFException.class, () -> inputStream.readFully(12, buffer1));
      assertEquals(currentPos, inputStream.getPos());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, 30 * 1024 * 1024, 30 * 1024 * 1024 + 1 })
  public void testByteBufferPositionedReadFullyWithInvalidPosition(int position) throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      long currentPos = inputStream.getPos();
      ByteBuffer buffer = ByteBuffer.allocate(20);
      assertThrows(EOFException.class, () -> inputStream.readFully(position, buffer));
      // File position should not be changed
      assertEquals(currentPos, inputStream.getPos());
    }
  }

  @Test
  public void testO3FSMultiByteRead() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      byte[] value = new byte[data.length];
      byte[] tmp = new byte[1 * 1024 * 1024];
      int i = 0;
      while (true) {
        int val = inputStream.read(tmp);
        if (val == -1) {
          break;
        }
        System.arraycopy(tmp, 0, value, i * tmp.length, tmp.length);
        i++;
      }
      assertEquals((long) i * tmp.length, data.length);
      assertArrayEquals(value, data);
    }
  }

  @Test
  public void testO3FSByteBufferRead() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {

      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
      int byteRead = inputStream.read(buffer);

      assertEquals(byteRead, 1024 * 1024);

      byte[] value = new byte[1024 * 1024];
      System.arraycopy(data, 0, value, 0, value.length);

      assertArrayEquals(value, buffer.array());
    }
  }

  @Test
  public void testSequenceFileReaderSync() throws IOException {
    File srcfile = new File("src/test/resources/testSequenceFile");
    Path path = new Path("/" + RandomStringUtils.secure().nextAlphanumeric(5));
    InputStream input = new BufferedInputStream(Files.newInputStream(srcfile.toPath()));

    // Upload test SequenceFile file
    FSDataOutputStream output = fs.create(path);
    IOUtils.copyBytes(input, output, 4096, true);
    input.close();

    // Start SequenceFile.Reader test
    SequenceFile.Reader in = new SequenceFile.Reader(fs, path, cluster().getConf());
    long blockStart = -1;
    // EOFException should not occur.
    in.sync(0);
    blockStart = in.getPosition();
    // The behavior should be consistent with HDFS
    assertEquals(srcfile.length(), blockStart);
    in.close();
  }

  @Test
  public void testSequenceFileReaderSyncEC() throws IOException {
    File srcfile = new File("src/test/resources/testSequenceFile");
    Path path = new Path("/" + RandomStringUtils.secure().nextAlphanumeric(5));
    InputStream input = new BufferedInputStream(Files.newInputStream(srcfile.toPath()));

    // Upload test SequenceFile file
    FSDataOutputStream output = ecFs.create(path);
    IOUtils.copyBytes(input, output, 4096, true);
    input.close();

    // Start SequenceFile.Reader test
    SequenceFile.Reader in = new SequenceFile.Reader(ecFs, path, cluster().getConf());
    long blockStart = -1;
    // EOFException should not occur.
    in.sync(0);
    blockStart = in.getPosition();
    // The behavior should be consistent with HDFS
    assertEquals(srcfile.length(), blockStart);
    in.close();
  }
}
