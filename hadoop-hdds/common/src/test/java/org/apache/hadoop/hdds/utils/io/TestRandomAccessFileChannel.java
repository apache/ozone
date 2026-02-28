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

package org.apache.hadoop.hdds.utils.io;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestRandomAccessFileChannel {
  @TempDir
  private Path tempDir;

  @Test
  void openFailureDoesNotLeaveOpenAndCloseIsSafe() {
    final RandomAccessFileChannel c = new RandomAccessFileChannel();
    final File missing = tempDir.resolve("missing-file").toFile();

    assertThrows(FileNotFoundException.class, () -> c.open(missing));
    assertFalse(c.isOpen());
    assertDoesNotThrow(c::close);
  }

  @Test
  void closeIsIdempotent() throws Exception {
    final RandomAccessFileChannel c = new RandomAccessFileChannel();
    final File f = tempDir.resolve("file").toFile();
    try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
      raf.write(1);
    }

    c.open(f);
    assertTrue(c.isOpen());

    assertDoesNotThrow(c::close);
    assertFalse(c.isOpen());
    assertDoesNotThrow(c::close);
  }

  @Test
  void closeContinuesToCloseRafEvenIfChannelCloseFails() throws Exception {
    final RandomAccessFileChannel c = new RandomAccessFileChannel();
    final File f = tempDir.resolve("file-to-close").toFile();
    try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
      raf.write(1);
    }

    final TrackingRandomAccessFile trackingRaf = new TrackingRandomAccessFile(f);
    setField(c, "blockFile", f);
    setField(c, "channel", new FailingCloseFileChannel());
    setField(c, "raf", trackingRaf);

    assertDoesNotThrow(c::close);
    assertTrue(trackingRaf.isClosed(), "raf.close() should still be called");
  }

  @Test
  void closeDoesNotThrowWhenRafAndChannelAreNull() throws Exception {
    final RandomAccessFileChannel c = new RandomAccessFileChannel();
    setField(c, "blockFile", tempDir.resolve("dummy").toFile());
    setField(c, "channel", null);
    setField(c, "raf", null);
    assertDoesNotThrow(c::close);
  }

  @Test
  void readWithZeroSizedBuffer() throws Exception {
    final RandomAccessFileChannel c = new RandomAccessFileChannel();
    final File f = tempDir.resolve("test-file").toFile();
    try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
      raf.write(new byte[]{1, 2, 3, 4, 5});
    }

    c.open(f);
    assertTrue(c.isOpen());

    final ByteBuffer zeroSizedBuffer = ByteBuffer.allocate(0);
    // Should return immediately without reading (buffer has no remaining capacity)
    assertTrue(c.read(zeroSizedBuffer), "read() should return true for zero-sized buffer");
    // Verify buffer state unchanged
    assertEquals(0, zeroSizedBuffer.remaining());
    assertEquals(0, zeroSizedBuffer.position());
    assertEquals(0, zeroSizedBuffer.limit());
  }

  private static void setField(Object target, String name, Object value)
      throws ReflectiveOperationException {
    final Field f = RandomAccessFileChannel.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }

  private static final class TrackingRandomAccessFile extends RandomAccessFile {
    private volatile boolean closed;

    private TrackingRandomAccessFile(File f) throws FileNotFoundException {
      super(f, "rw");
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }

    private boolean isClosed() {
      return closed;
    }
  }

  /**
   * {@link FileChannel#close()} is final (inherited), so we implement a minimal {@link FileChannel}
   * and throw from {@link #implCloseChannel()} to simulate close failure.
   */
  private static final class FailingCloseFileChannel extends FileChannel {
    @Override
    protected void implCloseChannel() throws IOException {
      throw new IOException("simulated close failure");
    }

    @Override
    public int read(ByteBuffer dst) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long position() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel position(long newPosition) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel truncate(long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void force(boolean metaData) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst, long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src, long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }
  }
}

