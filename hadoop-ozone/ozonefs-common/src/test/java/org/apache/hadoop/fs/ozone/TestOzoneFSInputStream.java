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

import static org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper.SOURCE_SIZE;
import static org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper.unwrapExecutionException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.scm.storage.ExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link OzoneFSInputStream}.
 */
public class TestOzoneFSInputStream {

  private static final byte CORRUPT_BYTE = (byte) 0x5A;

  private static final List<IntFunction<ByteBuffer>> BUFFER_CONSTRUCTORS =
      ImmutableList.of(ByteBuffer::allocate, ByteBuffer::allocateDirect);

  @Test
  public void readToByteBuffer() throws IOException {
    for (IntFunction<ByteBuffer> constructor : BUFFER_CONSTRUCTORS) {
      for (int streamLength = 1; streamLength <= 10; streamLength++) {
        for (int bufferCapacity = 0; bufferCapacity <= 10; bufferCapacity++) {
          testReadToByteBuffer(constructor, streamLength, bufferCapacity, 0);
          if (bufferCapacity > 1) {
            testReadToByteBuffer(constructor, streamLength, bufferCapacity, 1);
            if (bufferCapacity > 2) {
              testReadToByteBuffer(constructor, streamLength, bufferCapacity,
                  bufferCapacity - 1);
            }
          }
          testReadToByteBuffer(constructor, streamLength, bufferCapacity,
              bufferCapacity);
        }
      }
    }
  }

  private static void testReadToByteBuffer(
      IntFunction<ByteBuffer> bufferConstructor,
      int streamLength, int bufferCapacity,
      int bufferPosition) throws IOException {
    final byte[] source = RandomUtils.secure().randomBytes(streamLength);
    final InputStream input = new ByteArrayInputStream(source);
    final OzoneFSInputStream subject = createTestSubject(input);

    final int expectedReadLength = Math.min(bufferCapacity - bufferPosition,
        input.available());
    final byte[] expectedContent = Arrays.copyOfRange(source, 0,
        expectedReadLength);

    final ByteBuffer buf = bufferConstructor.apply(bufferCapacity);
    buf.position(bufferPosition);

    final int bytesRead = subject.read(buf);

    assertEquals(expectedReadLength, bytesRead);

    final byte[] content = new byte[bytesRead];
    buf.position(bufferPosition);
    buf.get(content);
    assertArrayEquals(expectedContent, content);
  }

  @Test
  public void readEmptyStreamToByteBuffer() throws IOException {
    for (IntFunction<ByteBuffer> constructor : BUFFER_CONSTRUCTORS) {
      final OzoneFSInputStream subject = createTestSubject(emptyStream());
      final ByteBuffer buf = constructor.apply(1);

      final int bytesRead = subject.read(buf);

      assertEquals(-1, bytesRead);
      assertEquals(0, buf.position());
    }
  }

  @Test
  public void bufferPositionUnchangedOnEOF() throws IOException {
    for (IntFunction<ByteBuffer> constructor : BUFFER_CONSTRUCTORS) {
      final OzoneFSInputStream subject = createTestSubject(eofStream());
      final ByteBuffer buf = constructor.apply(123);

      final int bytesRead = subject.read(buf);

      assertEquals(-1, bytesRead);
      assertEquals(0, buf.position());
    }
  }

  @Test
  public void testStreamCapability() throws IOException {
    final OzoneFSInputStream subject = createTestSubject(emptyStream());
    CapableOzoneFSInputStream capableOzoneFSInputStream = null;
    try {
      capableOzoneFSInputStream = new CapableOzoneFSInputStream(subject,
          new FileSystem.Statistics("test"));

      assertTrue(capableOzoneFSInputStream.
          hasCapability(StreamCapabilities.READBYTEBUFFER));
    } finally {
      if (capableOzoneFSInputStream != null) {
        capableOzoneFSInputStream.close();
      }
    }
  }

  @Test
  public void testCryptoStreamUnbuffer()
      throws IOException, GeneralSecurityException {
    KeyInputStream keyInputStream = mock(KeyInputStream.class);
    when(keyInputStream.hasCapability(anyString())).thenReturn(true);

    CryptoCodec codec = mock(CryptoCodec.class);
    when(codec.getCipherSuite()).thenReturn(CipherSuite.AES_CTR_NOPADDING);
    when(codec.getConf()).thenReturn(new Configuration());
    Decryptor decryptor = mock(Decryptor.class);
    when(codec.createDecryptor()).thenReturn(decryptor);
    CryptoInputStream cis = new CryptoInputStream(keyInputStream, codec,
        new byte[0], new byte[0]);
    try {
      cis.unbuffer();
      verify(keyInputStream, times(1)).unbuffer();
    } finally {
      cis.close();
    }
  }

  private static OzoneFSInputStream createTestSubject(InputStream input) {
    return new OzoneFSInputStream(input,
        new FileSystem.Statistics("test"));
  }

  private static InputStream emptyStream() {
    return new ByteArrayInputStream(new byte[0]);
  }

  private static InputStream eofStream() {
    return new InputStream() {
      @Override
      public int available() {
        return 123;
      }

      @Override
      public int read() {
        return -1;
      }
    };
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(value = 30)
  public void testConcurrentPositionedRead(boolean synchronizePositionedReads)
      throws Exception {
    final byte[] source = RandomUtils.secure().randomBytes(SOURCE_SIZE);
    final InterleavingSeekableInputStream underlying =
        new InterleavingSeekableInputStream(source);
    try (OzoneFSInputStream subject = new OzoneFSInputStream(underlying,
        new FileSystem.Statistics("test"), synchronizePositionedReads)) {

      if (synchronizePositionedReads) {
        PositionedReadTestHelper.runConcurrentPositionedReads(source,
            (offset, buf) -> subject.readFully(offset, buf));
      } else {
        ExecutionException executionException = assertThrows(
            ExecutionException.class,
            () -> PositionedReadTestHelper.runConcurrentPositionedReads(source,
                (offset, buf) -> subject.readFully(offset, buf)));
        assertInstanceOf(AssertionError.class,
            unwrapExecutionException(executionException));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(value = 30)
  public void testConcurrentPositionedReadEcFallback(boolean synchronizePositionedReads)
      throws Exception {
    final byte[] source = RandomUtils.secure().randomBytes(SOURCE_SIZE);
    final EcInterleavingInputStream underlying =
        new EcInterleavingInputStream(source);
    try (OzoneFSInputStream subject = new OzoneFSInputStream(underlying,
        new FileSystem.Statistics("test"), synchronizePositionedReads)) {

      if (synchronizePositionedReads) {
        PositionedReadTestHelper.runConcurrentPositionedReads(source,
            (offset, buf) -> subject.readFully(offset, buf));
      } else {
        ExecutionException executionException = assertThrows(
            ExecutionException.class,
            () -> PositionedReadTestHelper.runConcurrentPositionedReads(source,
                (offset, buf) -> subject.readFully(offset, buf)));
        assertInstanceOf(AssertionError.class,
            unwrapExecutionException(executionException));
      }
    }
  }

  /**
   * Mimics KeyInputStream synchronized per-operation seek/read where multi-steps
   * positioned reads must still be serialized at the FS layer.
   */
  private static final class InterleavingSeekableInputStream extends InputStream
      implements Seekable, org.apache.hadoop.fs.ByteBufferReadable {

    private final InterleavingReadState readState;

    private InterleavingSeekableInputStream(byte[] data) {
      this.readState = new InterleavingReadState(data);
    }

    @Override
    public synchronized void seek(long p) {
      readState.seek(p);
    }

    @Override
    public synchronized long getPos() {
      return readState.getPos();
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read() {
      return -1;
    }

    @Override
    public synchronized int read(ByteBuffer buf) {
      return readState.read(buf);
    }
  }

  /**
   * Mimics an erasure-coded key stream: {@link ExtendedInputStream#readFully}
   * returns {@code false}, so {@link OzoneFSInputStream} falls back to
   * seek-read-restore on the shared cursor.
   */
  private static final class EcInterleavingInputStream extends ExtendedInputStream {

    private final InterleavingReadState readState;

    private EcInterleavingInputStream(byte[] data) {
      this.readState = new InterleavingReadState(data);
    }

    @Override
    public boolean readFully(long position, ByteBuffer buffer) {
      return false;
    }

    @Override
    protected int readWithStrategy(ByteReaderStrategy strategy) {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void seek(long p) {
      readState.seek(p);
    }

    @Override
    public synchronized long getPos() {
      return readState.getPos();
    }

    @Override
    public synchronized boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public synchronized int read(ByteBuffer buf) {
      return readState.read(buf);
    }

    @Override
    public void unbuffer() {
      return;
    }
  }

  private static final class InterleavingReadState {
    private final byte[] data;
    private long pos;
    private final ThreadLocal<Long> expectedReadPos = new ThreadLocal<>();

    private InterleavingReadState(byte[] data) {
      this.data = data;
    }

    private void seek(long p) {
      pos = p;
      expectedReadPos.set(p);
    }

    private long getPos() {
      return pos;
    }

    private int read(ByteBuffer buf) {
      Long expected = expectedReadPos.get();
      if (expected != null && pos != expected) {
        int len = buf.remaining();
        for (int i = 0; i < len; i++) {
          buf.put(CORRUPT_BYTE);
        }
        return len;
      }
      int toRead = Math.min(buf.remaining(), data.length - (int) pos);
      if (toRead <= 0) {
        return -1;
      }
      buf.put(data, (int) pos, toRead);
      pos += toRead;
      expectedReadPos.remove();
      return toRead;
    }
  }

}
