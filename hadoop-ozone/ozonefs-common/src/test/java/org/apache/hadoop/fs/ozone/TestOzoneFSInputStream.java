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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

  @Test
  void cursorOnlyStreamsDoNotEmulatePositionedReads() throws Exception {
    byte[] source = RandomUtils.secure().randomBytes(32);
    try (CapableOzoneFSInputStream fs = new CapableOzoneFSInputStream(new SeekableOnlyInputStream(source), null);
         OzoneInputStream client = new OzoneInputStream(new SeekableOnlyInputStream(source))) {
      assertFalse(fs.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertFalse(client.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertFalse(client.hasCapability(StreamCapabilities.READBYTEBUFFER));
      assertFalse(client.hasCapability(StreamCapabilities.UNBUFFER));
      assertThrows(EOFException.class, () -> fs.read(-1, ByteBuffer.allocate(1)));
      assertThrows(UnsupportedOperationException.class, () -> fs.read(0, ByteBuffer.allocate(1)));
      assertThrows(UnsupportedOperationException.class, () -> client.read(0, ByteBuffer.allocate(1)));
      assertEquals(0, fs.getPos());
      assertEquals(0, client.getPos());
      assertEquals(Byte.toUnsignedInt(source[0]), fs.read());
      assertEquals(Byte.toUnsignedInt(source[0]), client.read());
    }
  }

  @Test
  void clientRespectsDelegateCapabilities() throws Exception {
    KeyInputStream key = mock(KeyInputStream.class);
    when(key.hasCapability(anyString())).thenReturn(false);
    try (OzoneInputStream client = new OzoneInputStream(key)) {
      assertFalse(client.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertFalse(client.hasCapability(StreamCapabilities.READBYTEBUFFER));
      assertFalse(client.hasCapability(StreamCapabilities.UNBUFFER));
    }
  }

  @Test
  @Timeout(30)
  void concurrentPositionedReadsThroughWrappers() throws Exception {
    byte[] source = RandomUtils.secure().randomBytes(SOURCE_SIZE);
    try (OzoneFSInputStream stream = createTestSubject(new OzoneInputStream(new NativePositionedInputStream(source)))) {
      PositionedReadTestHelper.runConcurrentPositionedReads(source, (offset, buffer) -> {
        if ((offset & 1) == 0) {
          stream.readFully(offset, buffer);
        } else {
          byte[] bytes = new byte[buffer.remaining()];
          stream.readFully(offset, bytes);
          buffer.put(bytes);
        }
      });
    }
  }

  @Test
  void positionedReadDelegatesThroughWrappersAndCountsBytesOnce() throws Exception {
    byte[] source = RandomUtils.secure().randomBytes(32);
    for (boolean wrapped : new boolean[] {false, true}) {
      FileSystem.Statistics statistics = new FileSystem.Statistics("test");
      InputStream input = new NativePositionedInputStream(source);
      if (wrapped) {
        input = new OzoneInputStream(input);
      }
      try (OzoneFSInputStream stream = new OzoneFSInputStream(input, statistics)) {
        byte[] result = new byte[12];
        stream.readFully(3, result, 2, 7);
        assertArrayEquals(Arrays.copyOfRange(source, 3, 10), Arrays.copyOfRange(result, 2, 9));
        assertEquals(7, statistics.getBytesRead());
        ByteBuffer destination = ByteBuffer.wrap(result, 2, 7).slice();
        assertEquals(3, stream.read(29, destination));
        assertArrayEquals(Arrays.copyOfRange(source, 29, 32), Arrays.copyOfRange(result, 2, 5));
        assertEquals(10, statistics.getBytesRead());
        assertEquals(-1, stream.read(32, ByteBuffer.allocateDirect(1)));
        assertThrows(EOFException.class, () -> stream.readFully(31, ByteBuffer.allocateDirect(2)));
        assertEquals(11, statistics.getBytesRead());
      }
    }
  }

  private static final class NativePositionedInputStream extends ByteArrayInputStream
      implements ByteBufferPositionedReadable {
    private NativePositionedInputStream(byte[] data) {
      super(data);
    }

    @Override
    public int read(long position, ByteBuffer destination) {
      if (position >= count) {
        return -1;
      }
      int n = Math.min(destination.remaining(), count - (int) position);
      destination.put(buf, (int) position, n);
      return n;
    }

    @Override
    public void readFully(long position, ByteBuffer destination) throws IOException {
      int length = destination.remaining();
      if (read(position, destination) < length) {
        throw new EOFException();
      }
    }
  }

  private static final class SeekableOnlyInputStream extends InputStream
      implements Seekable {

    private final byte[] data;
    private int pos;

    private SeekableOnlyInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public synchronized int read() {
      return pos < data.length ? (data[pos++] & 0xFF) : -1;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
      if (pos >= data.length) {
        return -1;
      }
      int n = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, b, off, n);
      pos += n;
      return n;
    }

    @Override
    public synchronized int available() {
      return data.length - pos;
    }

    @Override
    public synchronized void seek(long newPos) {
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
  }

}
