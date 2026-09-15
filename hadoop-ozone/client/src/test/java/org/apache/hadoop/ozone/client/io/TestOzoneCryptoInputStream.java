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

package org.apache.hadoop.ozone.client.io;

import static org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper.SOURCE_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.security.SecureRandom;
import java.util.Arrays;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link OzoneCryptoInputStream}.
 */
public class TestOzoneCryptoInputStream {

  private static final String KEY_NAME = "testKey";
  private static final int PART_INDEX = 0;
  // AES-128 key and IV
  private static final int KEY_LEN = 16;
  private static final int IV_LEN = 16;

  private static CryptoCodec codec;
  private static byte[] key;
  private static byte[] iv;

  @BeforeAll
  static void setup() throws Exception {
    Configuration conf = new Configuration();
    // Force JCE backend to avoid native library dependency in tests
    conf.set("hadoop.security.crypto.codec.classes.AES/CTR/NoPadding",
        "org.apache.hadoop.crypto.JceAesCtrCryptoCodec");
    codec = CryptoCodec.getInstance(conf);
    key = new byte[KEY_LEN];
    iv = new byte[IV_LEN];
    new SecureRandom().nextBytes(key);
    new SecureRandom().nextBytes(iv);
  }

  @Test
  void testPositionedReadAtStart() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(64 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      ByteBuffer buf = ByteBuffer.allocate(1024);
      int n = s.read(0, buf);
      buf.flip();
      assertArrayEquals(Arrays.copyOf(plaintext, n), toArray(buf));
    }
  }

  @Test
  void testPositionedReadInMiddle() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(64 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      int offset = 12345;
      ByteBuffer buf = ByteBuffer.allocate(2048);
      int n = s.read(offset, buf);
      buf.flip();
      assertArrayEquals(Arrays.copyOfRange(plaintext, offset, offset + n), toArray(buf));
    }
  }

  @Test
  void testPositionedReadCrossesBufferBoundary() throws Exception {
    // The crypto buffer is 8 KB by default; read crossing that boundary exercises
    // the position/length adjustment logic in OzoneCryptoInputStream.read(byte[], int, int).
    byte[] plaintext = RandomUtils.secure().randomBytes(32 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Start 100 bytes before the 8 KB boundary
      int offset = 8 * 1024 - 100;
      ByteBuffer buf = ByteBuffer.allocate(1024);
      int n = s.read(offset, buf);
      buf.flip();
      assertArrayEquals(Arrays.copyOfRange(plaintext, offset, offset + n), toArray(buf));
    }
  }

  @Test
  void testPositionedReadDoesNotMoveSequentialCursor() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(16 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Advance the sequential cursor
      s.seek(500);
      assertEquals(500, s.getPos());

      // Positioned read at a different offset
      ByteBuffer buf = ByteBuffer.allocate(256);
      s.read(8000, buf);

      // Sequential cursor must be restored
      assertEquals(500, s.getPos());
    }
  }

  @Test
  void testPositionedReadAtEof() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      ByteBuffer buf = ByteBuffer.allocate(64);
      assertEquals(-1, s.read(plaintext.length, buf),
          "read at position == length should return EOF");
      assertEquals(-1, s.read(plaintext.length + 1, buf),
          "read beyond length should return EOF");
    }
  }

  @Test
  void testPositionedReadEmptyBuffer() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      assertEquals(0, s.read(0, ByteBuffer.allocate(0)));
    }
  }

  @Test
  void testReadFullyBasic() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(64 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      int offset = 1000;
      int len = 8192;
      ByteBuffer buf = ByteBuffer.allocate(len);
      s.readFully(offset, buf);
      buf.flip();
      assertArrayEquals(Arrays.copyOfRange(plaintext, offset, offset + len), toArray(buf));
    }
  }

  @Test
  void testReadFullyDoesNotMoveSequentialCursor() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(32 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      s.seek(1234);
      ByteBuffer buf = ByteBuffer.allocate(512);
      s.readFully(16000, buf);
      assertEquals(1234, s.getPos(), "sequential cursor must be restored after readFully");
    }
  }

  @Test
  void testByteArrayReadNullBufferThrowsIAE() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Aligns with AbstractContractSeekTest.testReadNullBuffer: null must throw IAE, not NPE.
      assertThrows(IllegalArgumentException.class, () -> s.read(0, (byte[]) null, 0, 16));
    }
  }

  @Test
  void testByteArrayReadNegativePositionThrowsEOF() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Aligns with AbstractContractSeekTest.testReadSmallFile: negative position must throw.
      assertThrows(EOFException.class, () -> s.read(-1, new byte[16], 0, 16));
    }
  }

  @Test
  void testByteArrayReadFullyNullBufferThrowsIAE() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      assertThrows(IllegalArgumentException.class,
          () -> s.readFully(0, (byte[]) null, 0, 16));
    }
  }

  @Test
  void testByteArrayReadFullyNegativePositionThrowsEOF() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Aligns with AbstractContractSeekTest.testReadFullySmallFile: readFully must throw at invalid position.
      assertThrows(EOFException.class, () -> s.readFully(-1, new byte[16], 0, 16));
    }
  }

  @Test
  void testByteArrayReadAtEofReturnsMinusOne() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Position exactly at EOF must return -1, not throw.
      assertEquals(-1, s.read(plaintext.length, new byte[16], 0, 16));
    }
  }

  @Test
  void testByteBufferReadFullyThrowsEofWhenStreamTooShort() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(100);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Request 200 bytes starting at offset 50: 50 + 200 > 100 → EOFException
      ByteBuffer buf = ByteBuffer.allocate(200);
      assertThrows(EOFException.class, () -> s.readFully(50, buf));
    }
  }

  @Test
  void testByteArrayReadFullyThrowsEofWhenStreamTooShort() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(100);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      // Aligns with AbstractContractSeekTest.testReadFullySmallFile: partial buffer must throw.
      byte[] buf = new byte[200];
      assertThrows(EOFException.class, () -> s.readFully(50, buf, 0, buf.length));
    }
  }

  @Test
  void testPositionedReadRejectsReadOnlyBuffer() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(4 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      ByteBuffer readOnly = ByteBuffer.wrap(new byte[256]).asReadOnlyBuffer();
      assertThrows(ReadOnlyBufferException.class, () -> s.read(0, readOnly));
    }
  }

  @Test
  void testSequentialReadAfterReadOnlyRejection() throws Exception {
    // A ReadOnlyBufferException on read(long, ByteBuffer) must leave the stream in
    // a usable state: adjustment fields reset, cursor restored, next sequential read
    // returns correct data.
    byte[] plaintext = RandomUtils.secure().randomBytes(8 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      ByteBuffer readOnly = ByteBuffer.allocate(256).asReadOnlyBuffer();
      // Seek to a non-boundary offset so adjustReadPosition sets readPositionAdjustedBy.
      s.seek(100);
      assertThrows(ReadOnlyBufferException.class, () -> s.read(100, readOnly));
      // Cursor must be restored.
      assertEquals(100, s.getPos());
      // Adjustment fields must be reset — sequential read must not throw.
      byte[] buf = new byte[256];
      assertDoesNotThrow(() -> s.read(buf, 0, buf.length));
      assertArrayEquals(Arrays.copyOfRange(plaintext, 100, 356), buf);
    }
  }

  @Test
  void testByteArrayPositionedRead() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(32 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      int offset = 5000;
      byte[] buf = new byte[2048];
      int n = s.read(offset, buf, 0, buf.length);
      assertArrayEquals(Arrays.copyOfRange(plaintext, offset, offset + n),
          Arrays.copyOf(buf, n));
    }
  }

  @Test
  void testByteArrayReadFully() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(32 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      int offset = 3000;
      byte[] buf = new byte[4096];
      s.readFully(offset, buf, 0, buf.length);
      assertArrayEquals(Arrays.copyOfRange(plaintext, offset, offset + buf.length), buf);
    }
  }

  @Test
  void testByteArrayReadFullyShortForm() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(32 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      int offset = 1024;
      byte[] buf = new byte[512];
      s.readFully(offset, buf);
      assertArrayEquals(Arrays.copyOfRange(plaintext, offset, offset + buf.length), buf);
    }
  }

  @Test
  void testByteArrayReadFullyDoesNotMoveSequentialCursor() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(16 * 1024);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      s.seek(777);
      s.readFully(8000, new byte[256]);
      assertEquals(777, s.getPos(), "sequential cursor must be restored after readFully(byte[])");
    }
  }

  @Test
  @Timeout(value = 30)
  void testConcurrentSkipAndPositionedRead() throws Exception {
    // A concurrent skip must not slip between a positioned read's seek and
    // restore and corrupt the sequential cursor or the read result.
    byte[] plaintext = RandomUtils.secure().randomBytes(SOURCE_SIZE);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      PositionedReadTestHelper.runConcurrentPositionedReads(plaintext, (offset, buf) -> {
        if ((offset & 1) == 0) {
          s.readFully(offset, buf);
        } else {
          // Interleave skip(0) — a no-op skip that still acquires the monitor —
          // to exercise the happens-before between skip and positioned reads.
          s.skip(0);
          s.readFully(offset, buf);
        }
      });
    }
  }

  @Test
  @Timeout(value = 30)
  void testConcurrentPositionedReadsByteBuffer() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(SOURCE_SIZE);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      PositionedReadTestHelper.runConcurrentPositionedReads(plaintext,
          (offset, buf) -> s.readFully(offset, buf));
    }
  }

  @Test
  @Timeout(value = 30)
  void testConcurrentPositionedReadsByteArray() throws Exception {
    byte[] plaintext = RandomUtils.secure().randomBytes(SOURCE_SIZE);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      PositionedReadTestHelper.runConcurrentPositionedReads(plaintext, (offset, buf) -> {
        byte[] arr = new byte[buf.remaining()];
        s.readFully(offset, arr);
        buf.put(arr);
      });
    }
  }

  @Test
  @Timeout(value = 30)
  void testConcurrentPositionedReadsMixedApi() throws Exception {
    // Intermix ByteBuffer and byte-array callers on the same stream to verify
    // both APIs share the same monitor and do not interleave.
    byte[] plaintext = RandomUtils.secure().randomBytes(SOURCE_SIZE);
    try (OzoneCryptoInputStream s = buildStream(plaintext)) {
      PositionedReadTestHelper.runConcurrentPositionedReads(plaintext, (offset, buf) -> {
        if ((offset & 1) == 0) {
          s.readFully(offset, buf);
        } else {
          byte[] arr = new byte[buf.remaining()];
          s.readFully(offset, arr);
          buf.put(arr);
        }
      });
    }
  }

  /**
   * Encrypts {@code plaintext} with the shared key/IV, wraps it in a
   * {@link SeekableByteArrayInputStream}, and returns a ready-to-use
   * {@link OzoneCryptoInputStream}.
   */
  private static OzoneCryptoInputStream buildStream(byte[] plaintext) throws Exception {
    Encryptor enc = codec.createEncryptor();
    enc.init(key, iv);
    ByteBuffer plain = ByteBuffer.wrap(plaintext);
    ByteBuffer cipher = ByteBuffer.allocate(plaintext.length);
    enc.encrypt(plain, cipher);

    SeekableByteArrayInputStream raw = new SeekableByteArrayInputStream(cipher.array());
    LengthInputStream lin = new LengthInputStream(raw, plaintext.length);
    return new OzoneCryptoInputStream(lin, codec, key, iv, KEY_NAME, PART_INDEX);
  }

  private static byte[] toArray(ByteBuffer buf) {
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }

  /**
   * Seekable, in-memory {@link InputStream} backed by a byte array.
   * Used to feed ciphertext into {@link OzoneCryptoInputStream} in tests.
   */
  private static final class SeekableByteArrayInputStream extends InputStream
      implements Seekable, PositionedReadable {

    private final byte[] data;
    private volatile int pos;

    SeekableByteArrayInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public synchronized int read() throws IOException {
      return pos < data.length ? (data[pos++] & 0xFF) : -1;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
      if (pos >= data.length) {
        return -1;
      }
      int n = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, b, off, n);
      pos += n;
      return n;
    }

    @Override
    public synchronized void seek(long newPos) throws IOException {
      if (newPos < 0 || newPos > data.length) {
        throw new IOException("Invalid seek position: " + newPos);
      }
      pos = (int) newPos;
    }

    @Override
    public synchronized long getPos() {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      if (position >= data.length) {
        return -1;
      }
      int n = Math.min(length, data.length - (int) position);
      System.arraycopy(data, (int) position, buffer, offset, n);
      return n;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (read(position, buffer, offset, length) < length) {
        throw new EOFException("SeekableByteArrayInputStream: EOF at " + position);
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }
}
