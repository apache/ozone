/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Random;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyRead;
import static org.assertj.core.api.Assertions.fail;

/**
 * Test Seek operations.
 */
public abstract class AbstractContractSeekTest extends AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractSeekTest.class);

  public static final int DEFAULT_RANDOM_SEEK_COUNT = 100;

  private Path smallSeekFile;
  private Path zeroByteFile;
  private FSDataInputStream instream;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_SEEK);
    //delete the test directory
    smallSeekFile = path("seekfile.txt");
    zeroByteFile = path("zero.txt");
    byte[] block = dataset(TEST_FILE_LEN, 0, 255);
    //this file now has a simple rule: offset => value
    FileSystem fs = getFileSystem();
    createFile(fs, smallSeekFile, true, block);
    touch(fs, zeroByteFile);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, 4096);
    return conf;
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.closeStream(instream);
    instream = null;
    super.teardown();
  }

  /**
   * Skip a test case if the FS doesn't support positioned readable.
   * This should hold automatically if the FS supports seek, even
   * if it doesn't support seeking past the EOF.
   * And, because this test suite requires seek to be supported, the
   * feature is automatically assumed to be true unless stated otherwise.
   */
  protected void assumeSupportsPositionedReadable() throws IOException {
    // because this ,
    if (!getContract().isSupported(SUPPORTS_POSITIONED_READABLE, true)) {
      skip("Skipping as unsupported feature: "
          + SUPPORTS_POSITIONED_READABLE);
    }
  }

  @Test
  public void testSeekZeroByteFile() throws Throwable {
    describe("seek and read a 0 byte file");
    instream = getFileSystem().open(zeroByteFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    //expect initial read to fai;
    int result = instream.read();
    assertMinusOne("initial byte read", result);
    byte[] buffer = new byte[1];
    //expect that seek to 0 works
    instream.seek(0);
    //reread, expect same exception
    result = instream.read();
    assertMinusOne("post-seek byte read", result);
    result = instream.read(buffer, 0, 1);
    assertMinusOne("post-seek buffer read", result);
  }

  @Test
  public void testBlockReadZeroByteFile() throws Throwable {
    describe("do a block read on a 0 byte file");
    instream = getFileSystem().open(zeroByteFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    //expect that seek to 0 works
    byte[] buffer = new byte[1];
    int result = instream.read(buffer, 0, 1);
    assertMinusOne("block read zero byte file", result);
  }

  /**
   * Seek and read on a closed file.
   * Some filesystems let callers seek on a closed file -these must
   * still fail on the subsequent reads.
   * @throws Throwable
   */
  @Test
  public void testSeekReadClosedFile() throws Throwable {
    instream = getFileSystem().open(smallSeekFile);
    getLogger().debug("Stream is of type " + instream.getClass().getCanonicalName());
    instream.close();
    try {
      instream.seek(0);
      if (!isSupported(SUPPORTS_SEEK_ON_CLOSED_FILE)) {
        fail("seek succeeded on a closed stream");
      }
    } catch (IOException e) {
      //expected a closed file
    }
    try {
      int data = instream.available();
      if (!isSupported(SUPPORTS_AVAILABLE_ON_CLOSED_FILE)) {
        fail("available() succeeded on a closed stream, got " + data);
      }
    } catch (IOException e) {
      //expected a closed file
    }
    try {
      int data = instream.read();
      fail("read() succeeded on a closed stream, got " + data);
    } catch (IOException e) {
      //expected a closed file
    }
    try {
      byte[] buffer = new byte[1];
      int result = instream.read(buffer, 0, 1);
      fail("read(buffer, 0, 1) succeeded on a closed stream, got " + result);
    } catch (IOException e) {
      //expected a closed file
    }
    //what position does a closed file have?
    try {
      long offset = instream.getPos();
    } catch (IOException e) {
      // its valid to raise error here; but the test is applied to make
      // sure there's no other exception like an NPE.

    }
    //and close again
    instream.close();
  }

  @Test
  public void testNegativeSeek() throws Throwable {
    instream = getFileSystem().open(smallSeekFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    try {
      instream.seek(-1);
      long p = instream.getPos();
      LOG.warn("Seek to -1 returned a position of " + p);
      int result = instream.read();
      fail("expected an exception, got data " + result + " at a position of " + p);
    } catch (EOFException e) {
      //bad seek -expected
      handleExpectedException(e);
    } catch (IOException e) {
      //bad seek -expected, but not as preferred as an EOFException
      handleRelaxedException("a negative seek", "EOFException", e);
    }
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
  }

  @Test
  public void testSeekFile() throws Throwable {
    describe("basic seek operations");
    instream = getFileSystem().open(smallSeekFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    //expect that seek to 0 works
    instream.seek(0);
    int result = instream.read();
    Assertions.assertThat(result).isEqualTo(0);
    Assertions.assertThat(instream.read()).isEqualTo(1);
    Assertions.assertThat(instream.getPos()).isEqualTo(2);
    Assertions.assertThat(instream.read()).isEqualTo(2);
    Assertions.assertThat(instream.getPos()).isEqualTo(3);
    instream.seek(128);
    Assertions.assertThat(instream.getPos()).isEqualTo(128);
    Assertions.assertThat(instream.read()).isEqualTo(128);
    instream.seek(63);
    Assertions.assertThat(instream.read()).isEqualTo(63);
  }

  @Test
  public void testSeekAndReadPastEndOfFile() throws Throwable {
    describe("verify that reading past the last bytes in the file returns -1");
    instream = getFileSystem().open(smallSeekFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    //expect that seek to 0 works
    //go just before the end
    instream.seek(TEST_FILE_LEN - 2);
    Assertions.assertThat(instream.read() != -1).withFailMessage("Premature EOF").isTrue();
    Assertions.assertThat(instream.read() != -1).withFailMessage("Premature EOF").isTrue();
    assertMinusOne("read past end of file", instream.read());
  }

  @Test
  public void testSeekPastEndOfFileThenReseekAndRead() throws Throwable {
    describe("do a seek past the EOF, then verify the stream recovers");
    instream = getFileSystem().open(smallSeekFile);
    //go just before the end. This may or may not fail; it may be delayed until the
    //read
    boolean canSeekPastEOF =
        !getContract().isSupported(ContractOptions.REJECTS_SEEK_PAST_EOF, true);
    try {
      instream.seek(TEST_FILE_LEN + 1);
      //if this doesn't trigger, then read() is expected to fail
      assertMinusOne("read after seeking past EOF", instream.read());
    } catch (EOFException e) {
      //This is an error iff the FS claims to be able to seek past the EOF
      if (canSeekPastEOF) {
        //a failure wasn't expected
        throw e;
      }
      handleExpectedException(e);
    } catch (IOException e) {
      //This is an error iff the FS claims to be able to seek past the EOF
      if (canSeekPastEOF) {
        //a failure wasn't expected
        throw e;
      }
      handleRelaxedException("a seek past the end of the file",
          "EOFException", e);
    }
    //now go back and try to read from a valid point in the file
    instream.seek(1);
    Assertions.assertThat(instream.read() != -1).withFailMessage("Premature EOF").isTrue();
  }

  /**
   * Seek round a file bigger than IO buffers.
   * @throws Throwable
   */
  @Test
  public void testSeekBigFile() throws Throwable {
    describe("Seek round a large file and verify the bytes are what is expected");
    Path testSeekFile = path("bigseekfile.txt");
    byte[] block = dataset(100 * 1024, 0, 255);
    createFile(getFileSystem(), testSeekFile, true, block);
    instream = getFileSystem().open(testSeekFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    //expect that seek to 0 works
    instream.seek(0);
    int result = instream.read();
    Assertions.assertThat(result).isEqualTo(0);
    Assertions.assertThat(instream.read()).isEqualTo(1);
    Assertions.assertThat(instream.read()).isEqualTo(2);

    //do seek 32KB ahead
    instream.seek(32768);
    Assertions.assertThat((byte) instream.read()).withFailMessage("@32768").isEqualTo(block[32768]);
    instream.seek(40000);
    Assertions.assertThat((byte) instream.read()).withFailMessage("@40000").isEqualTo(block[40000]);
    instream.seek(8191);
    Assertions.assertThat((byte) instream.read()).withFailMessage("@8191").isEqualTo(block[8191]);
    instream.seek(0);
    Assertions.assertThat((byte) instream.read()).withFailMessage("@0").isEqualTo((byte) 0);

    // try read & readFully
    instream.seek(0);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    instream.read();
    Assertions.assertThat(instream.getPos()).isEqualTo(1);
    byte[] buf = new byte[80 * 1024];
    instream.readFully(1, buf, 0, buf.length);
    Assertions.assertThat(instream.getPos()).isEqualTo(1);
  }

  @Test
  public void testPositionedBulkReadDoesntChangePosition() throws Throwable {
    describe("verify that a positioned read does not change the getPos() value");
    assumeSupportsPositionedReadable();
    Path testSeekFile = path("bigseekfile.txt");
    byte[] block = dataset(65536, 0, 255);
    createFile(getFileSystem(), testSeekFile, true, block);
    instream = getFileSystem().open(testSeekFile);
    instream.seek(39999);
    Assertions.assertThat(instream.read()).isNotEqualTo(-1);
    Assertions.assertThat(instream.getPos()).isEqualTo(40000);

    int v = 256;
    byte[] readBuffer = new byte[v];
    instream.readFully(128, readBuffer, 0, v);
    //have gone back
    Assertions.assertThat(instream.getPos()).isEqualTo(40000);
    //content is the same too
    Assertions.assertThat((byte) instream.read()).withFailMessage("@40000").isEqualTo(block[40000]);
    //now verify the picked up data
    for (int i = 0; i < 256; i++) {
      Assertions.assertThat(readBuffer[i]).withFailMessage("@" + i).isEqualTo(block[i + 128]);
    }
  }

  /**
   * Lifted from TestLocalFileSystem:
   * Regression test for HADOOP-9307: BufferedFSInputStream returning
   * wrong results after certain sequences of seeks and reads.
   */
  @Test
  public void testRandomSeeks() throws Throwable {
    int limit = getContract().getLimit(TEST_RANDOM_SEEK_COUNT,
                                       DEFAULT_RANDOM_SEEK_COUNT);
    describe("Testing " + limit + " random seeks");
    int filesize = 10 * 1024;
    byte[] buf = dataset(filesize, 0, 255);
    Path randomSeekFile = path("testrandomseeks.bin");
    createFile(getFileSystem(), randomSeekFile, true, buf);
    Random r = new Random();

    // Record the sequence of seeks and reads which trigger a failure.
    int[] seeks = new int[10];
    int[] reads = new int[10];
    try (FSDataInputStream stm = getFileSystem().open(randomSeekFile)) {
      for (int i = 0; i < limit; i++) {
        int seekOff = r.nextInt(buf.length);
        int toRead = r.nextInt(Math.min(buf.length - seekOff, 32000));

        seeks[i % seeks.length] = seekOff;
        reads[i % reads.length] = toRead;
        verifyRead(stm, buf, seekOff, toRead);
      }
    } catch (AssertionError afe) {
      StringBuilder sb = new StringBuilder();
      sb.append("Sequence of actions:\n");
      for (int j = 0; j < seeks.length; j++) {
        sb.append("seek @ ").append(seeks[j]).append("  ")
            .append("read ").append(reads[j]).append("\n");
      }
      LOG.error(sb.toString());
      throw afe;
    }
  }

  @Test
  public void testReadFullyZeroByteFile() throws Throwable {
    describe("readFully against a 0 byte file");
    assumeSupportsPositionedReadable();
    instream = getFileSystem().open(zeroByteFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    byte[] buffer = new byte[1];
    instream.readFully(0, buffer, 0, 0);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    // seek to 0 read 0 bytes from it
    instream.seek(0);
    Assertions.assertThat(instream.read(buffer, 0, 0)).isEqualTo(0);
  }

  @Test
  public void testReadFullyPastEOFZeroByteFile() throws Throwable {
    assumeSupportsPositionedReadable();
    describe("readFully past the EOF of a 0 byte file");
    instream = getFileSystem().open(zeroByteFile);
    byte[] buffer = new byte[1];
    // try to read past end of file
    try {
      instream.readFully(0, buffer, 0, 16);
      fail("Expected an exception");
    } catch (IllegalArgumentException | IndexOutOfBoundsException
        | EOFException e) {
      // expected
    }
  }

  @Test
  public void testReadFullySmallFile() throws Throwable {
    describe("readFully operations");
    assumeSupportsPositionedReadable();
    instream = getFileSystem().open(smallSeekFile);
    byte[] buffer = new byte[256];
    // expect negative length to fail
    try {
      instream.readFully(0, buffer, 0, -16);
      fail("Expected an exception");
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      // expected
    }
    // negative offset into buffer
    try {
      instream.readFully(0, buffer, -1, 16);
      fail("Expected an exception");
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      // expected
    }
    // expect negative position to fail, ideally with EOF
    try {
      instream.readFully(-1, buffer);
      fail("Expected an exception");
    } catch (EOFException e) {
      handleExpectedException(e);
    } catch (IOException | IllegalArgumentException | IndexOutOfBoundsException e) {
      handleRelaxedException("readFully with a negative position ",
          "EOFException",
          e);
    }

    // read more than the offset allows
    try {
      instream.readFully(0, buffer, buffer.length - 8, 16);
      fail("Expected an exception");
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      // expected
    }

    // read properly
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    instream.readFully(0, buffer);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);

    // now read the entire file in one go
    byte[] fullFile = new byte[TEST_FILE_LEN];
    instream.readFully(0, fullFile);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);

    try {
      instream.readFully(16, fullFile);
      fail("Expected an exception");
    } catch (EOFException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("readFully which reads past EOF ",
          "EOFException",
          e);
    }
  }

  @Test
  public void testReadFullyPastEOF() throws Throwable {
    describe("readFully past the EOF of a file");
    assumeSupportsPositionedReadable();
    instream = getFileSystem().open(smallSeekFile);
    byte[] buffer = new byte[256];

    // now read past the end of the file
    try {
      instream.readFully(TEST_FILE_LEN + 1, buffer);
      fail("Expected an exception");
    } catch (EOFException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("readFully with an offset past EOF ",
          "EOFException",
          e);
    }
    // read zero bytes from an offset past EOF.
    try {
      instream.readFully(TEST_FILE_LEN + 1, buffer, 0, 0);
      // a zero byte read may fail-fast
      LOG.info("Filesystem short-circuits 0-byte reads");
    } catch (EOFException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("readFully(0 bytes) with an offset past EOF ",
          "EOFException",
          e);
    }
  }

  @Test
  public void testReadFullyZeroBytebufferPastEOF() throws Throwable {
    describe("readFully zero bytes from an offset past EOF");
    assumeSupportsPositionedReadable();
    instream = getFileSystem().open(smallSeekFile);
    byte[] buffer = new byte[256];
    try {
      instream.readFully(TEST_FILE_LEN + 1, buffer, 0, 0);
      // a zero byte read may fail-fast
      LOG.info("Filesystem short-circuits 0-byte reads");
    } catch (EOFException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("readFully(0 bytes) with an offset past EOF ",
          "EOFException",
          e);
    }
  }

  @Test
  public void testReadNullBuffer() throws Throwable {
    describe("try to read a null buffer ");
    assumeSupportsPositionedReadable();
    try (FSDataInputStream in = getFileSystem().open(smallSeekFile)) {
      // Null buffer
      int r = in.read(0, null, 0, 16);
      fail("Expected an exception from a read into a null buffer, got " + r);
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testReadSmallFile() throws Throwable {
    describe("PositionedRead.read operations");
    assumeSupportsPositionedReadable();
    instream = getFileSystem().open(smallSeekFile);
    byte[] buffer = new byte[256];
    int r;
    // expect negative length to fail
    try {
      r = instream.read(0, buffer, 0, -16);
      fail("Expected an exception, got " + r);
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      // expected
    }
    // negative offset into buffer
    try {
      r = instream.read(0, buffer, -1, 16);
      fail("Expected an exception, got " + r);
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      // expected
    }
    // negative position
    try {
      r = instream.read(-1, buffer, 0, 16);
      fail("Expected an exception, got " + r);
    } catch (EOFException e) {
      handleExpectedException(e);
    } catch (IOException | IllegalArgumentException | IndexOutOfBoundsException e) {
      handleRelaxedException("read() with a negative position ",
          "EOFException",
          e);
    }

    // read more than the offset allows
    try {
      r = instream.read(0, buffer, buffer.length - 8, 16);
      fail("Expected an exception, got " + r);
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      // expected
    }

    // read properly
    Assertions.assertThat(instream.getPos()).isEqualTo(0);
    instream.readFully(0, buffer);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);

    // now read the entire file in one go
    byte[] fullFile = new byte[TEST_FILE_LEN];
    instream.readFully(0, fullFile, 0, fullFile.length);
    Assertions.assertThat(instream.getPos()).isEqualTo(0);

    // now read past the end of the file
    Assertions.assertThat(instream.read(TEST_FILE_LEN + 16, buffer, 0, 1))
        .isEqualTo(-1);
  }

  @Test
  public void testReadAtExactEOF() throws Throwable {
    describe("read at the end of the file");
    instream = getFileSystem().open(smallSeekFile);
    instream.seek(TEST_FILE_LEN - 1);
    Assertions.assertThat(instream.read() > 0).withFailMessage("read at last byte").isTrue();
    Assertions.assertThat(instream.read()).withFailMessage("read just past EOF").isEqualTo(-1);
  }
}
