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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
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
import java.nio.ReadOnlyBufferException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import javax.crypto.CipherInputStream;
import javax.crypto.NullCipher;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.scm.storage.ExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OzoneFSInputStream}.
 */
public class TestOzoneFSInputStream {

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
    final OzoneFSInputStream subject = createTestSubject(new NativePositionedInputStream(new byte[0]));
    CapableOzoneFSInputStream capableOzoneFSInputStream = null;
    try {
      capableOzoneFSInputStream = new CapableOzoneFSInputStream(subject,
          new FileSystem.Statistics("test"));

      assertTrue(capableOzoneFSInputStream.
          hasCapability(StreamCapabilities.READBYTEBUFFER));
      assertTrue(capableOzoneFSInputStream.hasCapability(StreamCapabilities.VECTOREDIO));
    } finally {
      if (capableOzoneFSInputStream != null) {
        capableOzoneFSInputStream.close();
      }
    }
  }

  @Test
  void vectoredReadsOverlapAndAllocateOnlyForActiveWorkers() throws Exception {
    ExtendedInputStream input = mock(ExtendedInputStream.class);
    when(input.hasCapability(StreamCapabilities.PREADBYTEBUFFER)).thenReturn(true);
    CountDownLatch started = new CountDownLatch(4);
    CountDownLatch proceed = new CountDownLatch(1);
    CountDownLatch released = new CountDownLatch(1);
    AtomicInteger allocations = new AtomicInteger();
    when(input.read(anyLong(), any(ByteBuffer.class))).thenAnswer(inv -> {
      started.countDown();
      assertTrue(proceed.await(10, TimeUnit.SECONDS));
      long offset = inv.getArgument(0);
      ByteBuffer buffer = inv.getArgument(1);
      int length = buffer.remaining();
      for (int i = 0; i < length; i++) {
        buffer.put((byte) (offset + i));
      }
      return length;
    });
    List<FileRange> ranges = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      ranges.add(FileRange.createFileRange(i * 8, 4));
    }
    FileSystem.Statistics statistics = new FileSystem.Statistics("vector");
    try (CapableOzoneFSInputStream stream = new CapableOzoneFSInputStream(input, statistics)) {
      stream.readVectored(ranges, length -> {
        allocations.incrementAndGet();
        return ByteBuffer.allocateDirect(length);
      }, buffer -> released.countDown());
      assertTrue(started.await(5, TimeUnit.SECONDS), "four reads must overlap");
      assertEquals(4, allocations.get());
      for (FileRange range : ranges) {
        assertNotNull(range.getData());
        assertFalse(range.getData().isDone());
      }
      // Cancellation while a read is active must return its allocated buffer.
      ranges.get(0).getData().cancel(false);
      proceed.countDown();
      for (int i = 1; i < ranges.size(); i++) {
        ByteBuffer data = ranges.get(i).getData().get(5, TimeUnit.SECONDS);
        assertEquals(4, data.remaining());
        for (int j = 0; j < 4; j++) {
          assertEquals((byte) (i * 8 + j), data.get());
        }
      }
      assertTrue(released.await(5, TimeUnit.SECONDS));
      assertEquals(80, statistics.getBytesRead());
    } finally {
      proceed.countDown();
    }
  }

  @Test
  void vectoredReadsPreserveCursorAndReleaseFailedBuffers() throws Exception {
    byte[] source = RandomUtils.secure().randomBytes(32);
    for (IntFunction<ByteBuffer> allocate : BUFFER_CONSTRUCTORS) {
      AtomicInteger released = new AtomicInteger();
      FileRange success = FileRange.createFileRange(3, 4);
      FileRange empty = FileRange.createFileRange(12, 0);
      FileRange eof = FileRange.createFileRange(28, 8);
      try (CapableOzoneFSInputStream stream = new CapableOzoneFSInputStream(
          new NativePositionedInputStream(source), null)) {
        stream.seek(17);
        stream.readVectored(Arrays.asList(eof, empty, success), allocate, buffer -> {
          released.incrementAndGet();
          throw new IllegalStateException("release failed");
        });
        ByteBuffer data = success.getData().get(5, TimeUnit.SECONDS);
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        assertArrayEquals(Arrays.copyOfRange(source, 3, 7), bytes);
        assertEquals(0, empty.getData().get(5, TimeUnit.SECONDS).remaining());
        ExecutionException error = assertThrows(ExecutionException.class, () -> eof.getData().get(5, TimeUnit.SECONDS));
        assertTrue(error.getCause() instanceof EOFException);
        assertEquals(1, error.getCause().getSuppressed().length);
        assertEquals(1, released.get());
        assertEquals(17, stream.getPos());
      }
    }
  }

  @Test
  void vectoredReadsValidateBeforeSchedulingAndReportAllocationFailure() throws Exception {
    try (CapableOzoneFSInputStream stream = new CapableOzoneFSInputStream(new NativePositionedInputStream(new byte[32]),
        null)) {
      assertThrows(IllegalArgumentException.class, () -> stream.readVectored(
          Arrays.asList(FileRange.createFileRange(0, 4), FileRange.createFileRange(3, 4)), ByteBuffer::allocate));
      assertThrows(EOFException.class, () -> stream.readVectored(
          Collections.singletonList(FileRange.createFileRange(-1, 4)), ByteBuffer::allocate));
      stream.readVectored(Collections.emptyList(), ByteBuffer::allocate);
      FileRange failed = FileRange.createFileRange(0, 4);
      FileRange success = FileRange.createFileRange(10, 2);
      stream.readVectored(Arrays.asList(failed, success), length -> {
        if (length == 4) {
          throw new IllegalStateException("allocation failed");
        }
        return ByteBuffer.allocate(length);
      });
      assertThrows(ExecutionException.class, () -> failed.getData().get(5, TimeUnit.SECONDS));
      assertEquals(2, success.getData().get(5, TimeUnit.SECONDS).remaining());
    }
  }

  @Test
  void closingStreamFailsPendingVectoredRangesWithoutAllocatingBuffers() throws Exception {
    CountDownLatch started = new CountDownLatch(4);
    CountDownLatch proceed = new CountDownLatch(1);
    AtomicInteger allocations = new AtomicInteger();
    ExtendedInputStream input = mock(ExtendedInputStream.class);
    when(input.hasCapability(StreamCapabilities.PREADBYTEBUFFER)).thenReturn(true);
    when(input.read(anyLong(), any(ByteBuffer.class))).thenAnswer(inv -> {
      started.countDown();
      assertTrue(proceed.await(10, TimeUnit.SECONDS));
      throw new IOException("Stream is closed");
    });
    List<FileRange> ranges = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      ranges.add(FileRange.createFileRange(i, 1));
    }
    try (CapableOzoneFSInputStream stream = new CapableOzoneFSInputStream(input, null)) {
      stream.readVectored(ranges, length -> {
        allocations.incrementAndGet();
        return ByteBuffer.allocate(length);
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      stream.close();
      proceed.countDown();
      for (FileRange range : ranges) {
        assertThrows(ExecutionException.class, () -> range.getData().get(5, TimeUnit.SECONDS));
      }
      assertEquals(4, allocations.get());
      assertThrows(IOException.class, () -> stream.readVectored(ranges, ByteBuffer::allocate));
    } finally {
      proceed.countDown();
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
      assertFalse(fs.hasCapability(StreamCapabilities.VECTOREDIO));
      assertThrows(UnsupportedOperationException.class, () -> fs.readVectored(
          Collections.singletonList(FileRange.createFileRange(0, 1)), ByteBuffer::allocate));
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
  void gdprDelegateDoesNotSupportPositionedReads() throws Exception {
    byte[] data = new byte[] {1, 2, 3};
    try (OzoneInputStream client = new OzoneInputStream(new CipherInputStream(new ByteArrayInputStream(data),
        new NullCipher()));
         CapableOzoneFSInputStream fs = new CapableOzoneFSInputStream(client.getInputStream(), null)) {
      assertFalse(fs.hasCapability(StreamCapabilities.PREADBYTEBUFFER));
      assertThrows(UnsupportedOperationException.class, () -> fs.read(0, new byte[1], 0, 1));
      byte[] actual = new byte[data.length];
      assertEquals(data.length, fs.read(actual));
      assertArrayEquals(data, actual);
    }
  }

  @Test
  void positionedReadsValidateEmptyRequests() throws Exception {
    byte[] data = new byte[8];
    try (OzoneInputStream client = new OzoneInputStream(new NativePositionedInputStream(data));
         OzoneFSInputStream fs = createTestSubject(client)) {
      for (ByteBufferPositionedReadable stream : Arrays.asList(client, fs)) {
        ByteBuffer empty = ByteBuffer.allocate(0);
        ByteBuffer readOnly = empty.asReadOnlyBuffer();
        assertThrows(EOFException.class, () -> stream.read(-1, empty));
        assertThrows(EOFException.class, () -> stream.readFully(-1, empty));
        assertThrows(ReadOnlyBufferException.class, () -> stream.read(0, readOnly));
        assertThrows(ReadOnlyBufferException.class, () -> stream.readFully(0, readOnly));
        assertThrows(NullPointerException.class, () -> stream.read(0, null));
        assertThrows(NullPointerException.class, () -> stream.readFully(0, null));
        PositionedReadable arrayStream = (PositionedReadable) stream;
        assertThrows(EOFException.class, () -> arrayStream.read(-1, data, 0, 0));
        assertThrows(EOFException.class, () -> arrayStream.readFully(-1, data, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> arrayStream.read(0, null, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> arrayStream.readFully(0, null, 0, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> arrayStream.read(0, data, -1, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> arrayStream.readFully(0, data, -1, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> arrayStream.read(0, data, data.length + 1, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> arrayStream.readFully(0, data, data.length + 1, 0));
        for (long position : new long[] {0, data.length, data.length + 1}) {
          assertEquals(0, stream.read(position, empty));
          stream.readFully(position, empty);
          assertEquals(0, arrayStream.read(position, data, data.length, 0));
          arrayStream.readFully(position, data, data.length, 0);
        }
      }
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
      implements ByteBufferPositionedReadable, Seekable {
    private NativePositionedInputStream(byte[] data) {
      super(data);
    }

    @Override
    public void seek(long offset) {
      pos = (int) offset;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
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
