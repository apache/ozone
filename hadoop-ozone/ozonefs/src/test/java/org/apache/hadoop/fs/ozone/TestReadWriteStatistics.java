/*
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

package org.apache.hadoop.fs.ozone;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * Tests to check if bytes read and written and corresponding read and write
 * operation counts are accounted properly in FileSystem statistics, when the
 * FileSystem API is used to read data from Ozone.
 */
public class TestReadWriteStatistics {

  private Path aPath = new Path("/afile");
  private byte[] buff = new byte[512];

  @Test
  public void testZeroBytesReadWhenExceptionWasThrown() throws Exception {
    setupFakeInputStreamToThrowIOExceptionOnRead();
    FSDataInputStream stream = fs.open(aPath);

    try {
      stream.read();
    } catch (IOException e){
      // Expected
    }

    assertBytesReadAndReadNumOps(0, 1);
  }

  @Test
  public void testZeroBytesReadWhenEOFReached() throws Exception {
    setupFakeInputStreamToReadByte(-1);
    FSDataInputStream stream = fs.open(aPath);

    stream.read();

    assertBytesReadAndReadNumOps(0, 1);
  }

  @Test
  public void testOneByteReadOnSingleReadCall() throws Exception {
    setupFakeInputStreamToReadByte(20);
    FSDataInputStream stream = fs.open(aPath);

    stream.read();

    assertBytesReadAndReadNumOps(1, 1);
  }

  @Test
  public void testConsecutiveReadsIncreaseStats() throws Exception {
    setupFakeInputStreamToReadByte(20);
    FSDataInputStream stream = fs.open(aPath);

    for (int i = 1; i <= 5; i++) {
      stream.read();

      assertBytesReadAndReadNumOps(i, 1);
    }
  }

  @Test
  public void testConsecutiveOpensAndReadsIncreaseStats() throws Exception {
    setupFakeInputStreamToReadByte(20);

    for (int i = 0; i < 5; i++) {
      FSDataInputStream stream = fs.open(aPath);
      stream.read();
      stream.close();

      assertBytesReadAndReadNumOps(i+1, i+1);
    }
  }

  @Test
  public void testConsecutiveOpensIncreaseStats() throws Exception {
    setupFakeInputStreamToReadByte(20);

    for (int i = 1; i <= 5; i++) {
      FSDataInputStream stream = fs.open(aPath);
      stream.close();

      assertBytesReadAndReadNumOps(0, i);
    }
  }

  @Test
  public void testZeroBytesReadOnMultiByteReadWhenExceptionWasThrown()
      throws Exception {
    setupFakeInputStreamToThrowExceptionOnMultiByteRead();
    FSDataInputStream stream = fs.open(aPath);

    try {
      stream.read(buff, 0, buff.length);
    } catch (IOException e) {
      // Expected
    }

    assertBytesReadAndReadNumOps(0, 1);
  }

  @Test
  public void testZeroBytesReadOnMultiByteReadWhenEOFReachedAtStart()
      throws Exception {
    setupFakeInputStreamToReadNumBytesOnMultiByteRead(-1);
    FSDataInputStream stream = fs.open(aPath);

    stream.read(buff, 0, buff.length);

    assertBytesReadAndReadNumOps(0, 1);
  }

  @Test
  public void testEOFBeforeLengthOnMultiByteRead() throws Exception {
    setupFakeInputStreamToReadNumBytesOnMultiByteRead(256);
    FSDataInputStream stream = fs.open(aPath);

    stream.read(buff, 0, buff.length);

    assertBytesReadAndReadNumOps(256, 1);
  }

  @Test
  public void testFullyReadBufferOnMultiByteRead() throws Exception {
    setupFakeInputStreamToReadNumBytesOnMultiByteRead(buff.length);
    FSDataInputStream stream = fs.open(aPath);

    stream.read(buff, 0, buff.length);

    assertBytesReadAndReadNumOps(buff.length, 1);
  }

  @Test
  public void testConsecutiveReadsToBufferOnMultiByteRead() throws Exception {
    setupFakeInputStreamToReadNumBytesOnMultiByteRead(buff.length);
    FSDataInputStream stream = fs.open(aPath);

    stream.read(buff, 0, buff.length);
    stream.read(buff, 0, buff.length);
    assertBytesReadAndReadNumOps(2*buff.length, 1);

    setupFakeInputStreamToReadNumBytesOnMultiByteRead(256);
    stream.read(buff, 0, 256);

    assertBytesReadAndReadNumOps(2*buff.length + 256, 1);
  }

  @Test
  public void testZeroBytesWrittenWhenExceptionWasThrown() throws Exception {
    setupFakeOutputStreamToThrowIOExceptionOnWrite();
    FSDataOutputStream stream = fs.create(aPath);

    try {
      stream.write(20);
    } catch (IOException e) {
      //Expected
    }

    assertBytesWrittenAndWriteNumOps(0, 1);
  }

  @Test
  public void testOneByteWrittenOnSingleWriteCall() throws Exception {
    FSDataOutputStream stream = fs.create(aPath);

    stream.write(20);

    assertBytesWrittenAndWriteNumOps(1, 1);
  }

  @Test
  public void testConsecutiveWritesIncreaseStats() throws Exception {
    FSDataOutputStream stream = fs.create(aPath);

    for(int i = 1; i <= 5; i++){
      stream.write(20);

      assertBytesWrittenAndWriteNumOps(i, 1);
    }
  }

  @Test
  public void testConsecutiveCreatesAndWritesIncreaseStats() throws Exception {
    for(int i = 1; i <= 5; i++){
      FSDataOutputStream stream = fs.create(aPath);

      stream.write(20);

      assertBytesWrittenAndWriteNumOps(i, i);
    }
  }

  @Test
  public void testConsecutiveCreatesIncreaseStats() throws Exception {
    for(int i = 1; i <= 5; i++){
      fs.create(aPath);

      assertBytesWrittenAndWriteNumOps(0, i);
    }
  }

  @Test
  public void testBufferReadCallsIncreaseStatistics()
      throws Exception {
    setupFakeInputStreamToReadNumBytesOnMultiByteRead(128);
    ByteBuffer buffer = ByteBuffer.wrap(buff);
    FSDataInputStream stream = fs.open(aPath);

    stream.read(buffer);

    assertBytesReadAndReadNumOps(128, 1);
  }

  @Test
  public void testReadToReadOnlyBufferDoesNotChangeStats() throws Exception {
    setupFakeInputStreamToReadNumBytesOnMultiByteRead(128);
    ByteBuffer buffer = ByteBuffer.wrap(buff).asReadOnlyBuffer();
    FSDataInputStream stream = fs.open(aPath);

    try {
      stream.read(buffer);
    } catch (ReadOnlyBufferException e) {
      // Expected
    }

    assertBytesReadAndReadNumOps(0, 1);
  }

  @Test
  public void testZeroBytesWrittenOnMultiByteWriteWhenExceptionWasThrown()
      throws Exception {
    setupFakeOutputStreamToThrowIOExceptionOnMultiByteWrite();
    FSDataOutputStream stream = fs.create(aPath);

    try {
      stream.write(buff, 0, buff.length);
    } catch (IOException e) {
      // Expected
    }

    assertBytesWrittenAndWriteNumOps(0, 1);
  }

  @Test
  public void testBufferFullyWrittenOnMultiByteWrite() throws Exception {
    FSDataOutputStream stream = fs.create(aPath);

    stream.write(buff, 0, buff.length);

    assertBytesWrittenAndWriteNumOps(buff.length, 1);
  }

  @Test
  public void testBufferPartiallyWrittenOnMultiByteWrite() throws Exception {
    FSDataOutputStream stream = fs.create(aPath);

    stream.write(buff, buff.length/2, buff.length/4);

    assertBytesWrittenAndWriteNumOps(buff.length/4, 1);
  }

  @Test
  public void testConsecutiveMultiByteWritesIncreaseStats() throws Exception {
    FSDataOutputStream stream = fs.create(aPath);

    for(int i = 1; i <=5; i++) {
      stream.write(buff, 0, buff.length);
      assertBytesWrittenAndWriteNumOps((i * buff.length), 1);
    }

    stream.write(buff, 0, 128);
    assertBytesWrittenAndWriteNumOps((128 + (5 * buff.length)), 1);
  }

  @Test
  public void testNonRecursiveCreateIncreaseStats() throws Exception {
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.OVERWRITE);
    for(int i = 1; i <=5; i++){
      FSDataOutputStream stream =
          fs.createNonRecursive(aPath, null, flags, 512, (short) 3, 512, null);

      assertBytesWrittenAndWriteNumOps(0, i);
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testsIfAppendGetsSupported() throws Exception {
    fs.append(aPath, 512, null);
    fail("Add tests to cover metrics changes on append!");
  }

  // INTERNALS
  //TODO: check on this why it is not equals to OzoneFSStorageStatistics.NAME
  // as I believe this should be the value instead of the seen one.

  private static final String O3FS_STORAGE_STAT_NAME = "o3fs";
  // See Hadoop main project's FileSystemStorageStatistics class for the KEYS
  // there are these defined as well.
  private static final String STAT_NAME_BYTES_READ = "bytesRead";

  private static final String STAT_NAME_BYTES_WRITTEN = "bytesWritten";
  private static final String STAT_NAME_READ_OPS = "readOps";
  private static final String STAT_NAME_LARGE_READ_OPS = "largeReadOps";
  private static final String STAT_NAME_WRITE_OPS = "writeOps";
  // These are out of scope at this time, included here in comment to have the
  // full list of possible keys.
  // private static final String STAT_NAME_BYTES_READ_LOCALHOST = "";
  // private static final String STAT_NAME_BYTES_READ_DISTANCE_1_OR_2 = "";
  // private static final String STAT_NAME_BYTES_READ_DISTANCE_3_OR_4 = "";
  // private static final String STAT_NAME_BYTES_READ_DISTANCE_5_OR_LARGER = "";
  // private static final String STAT_NAME_BYTES_READ_ERASURE_CODED = "";

  private long readValueFromFSStatistics(String valueName) {
    GlobalStorageStatistics stats = FileSystem.getGlobalStorageStatistics();
    StorageStatistics fsStats = stats.get(O3FS_STORAGE_STAT_NAME);
    return fsStats.getLong(valueName);
  }

  private void assertBytesReadAndReadNumOps(
      long expectedBytesRead, long expectedNumReadOps) {

    long bytesRead = readValueFromFSStatistics(STAT_NAME_BYTES_READ);
    long numReadOps = readValueFromFSStatistics(STAT_NAME_READ_OPS);
    assertEquals("Bytes read.", expectedBytesRead, bytesRead);
    assertEquals("Read op count.", expectedNumReadOps, numReadOps);
  }

  private void assertBytesWrittenAndWriteNumOps(
      long expectedBytesWritten, long expectedNumWriteOps) {

    long bytesWritten = readValueFromFSStatistics(STAT_NAME_BYTES_WRITTEN);
    long numWriteOps = readValueFromFSStatistics(STAT_NAME_WRITE_OPS);
    assertEquals("Bytes written.", expectedBytesWritten, bytesWritten);
    assertEquals("Write op count.", expectedNumWriteOps, numWriteOps);
  }


  // TEST SETUP
  private OzoneFileSystem fs = spy(new OzoneFileSystem());

  private OzoneClientAdapter fakeAdapter = mock(OzoneClientAdapter.class);

  //we need a Seekable here to check readFully comfortably
  private InputStream fakeInputStream = mock(InputStream.class);

  private OutputStream fakeOutputStream = mock(OutputStream.class);

  @Before
  public void setupMocks() throws Exception {
    setupAdapterToReturnFakeInputStreamOnReadFile();
    setupAdapterToReturnFakeOutputStreamOnCreate();
    setupFileSystemToUseFakeClientAdapter();
    initializeFS();
    Arrays.fill(buff, (byte) 20);
  }

  private void setupAdapterToReturnFakeInputStreamOnReadFile()
      throws IOException {
    when(fakeAdapter.readFile(anyString())).thenReturn(fakeInputStream);
  }

  private void setupAdapterToReturnFakeOutputStreamOnCreate() throws Exception {
    when(fakeAdapter.createFile(anyString(), anyBoolean(), anyBoolean()))
        .thenReturn(new OzoneFSOutputStream(fakeOutputStream));
  }

  private void setupFileSystemToUseFakeClientAdapter() throws IOException {
    doReturn(fakeAdapter).when(fs).createAdapter(any(Configuration.class),
        anyString(), anyString(), anyString(), anyInt(), anyBoolean());
  }

  private void initializeFS() throws IOException, URISyntaxException {
    FileSystem.getGlobalStorageStatistics().reset();
    URI fsUri = new URI("o3fs://volume.bucket.localhost");
    Configuration conf = new Configuration();
    fs.initialize(fsUri, conf);
  }

  private void setupFakeInputStreamToThrowIOExceptionOnRead()
      throws IOException {
    when(fakeInputStream.read()).thenThrow(new IOException("Simulated IOE"));
  }

  private void setupFakeInputStreamToReadByte(int byteToReturn)
      throws IOException {
    when(fakeInputStream.read()).thenReturn(byteToReturn);
  }

  private void setupFakeInputStreamToThrowExceptionOnMultiByteRead()
      throws Exception {
    when(fakeInputStream.read(any(byte[].class), anyInt(), anyInt()))
        .thenThrow(new IOException("Simulated IOE"));
  }

  private void setupFakeInputStreamToReadNumBytesOnMultiByteRead(
      int numOfBytesToReturn) throws Exception {
    when(fakeInputStream.read(any(byte[].class), anyInt(), anyInt()))
        .thenReturn(numOfBytesToReturn);
  }

  private void setupFakeOutputStreamToThrowIOExceptionOnWrite()
      throws Exception {
    doThrow(new IOException("Simulated IOE"))
        .when(fakeOutputStream).write(anyInt());
  }

  private void setupFakeOutputStreamToThrowIOExceptionOnMultiByteWrite()
      throws Exception {
    doThrow(new IOException("Simulated IOE"))
        .when(fakeOutputStream).write(any(byte[].class), anyInt(), anyInt());
  }

}
