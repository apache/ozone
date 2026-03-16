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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    final FileChannel failingChannel = mock(FileChannel.class);
    doThrow(new IOException("simulated close failure")).when(failingChannel).close();
    final RandomAccessFile spyRaf = spy(new RandomAccessFile(f, "rw"));
    setField(c, "blockFile", f);
    setField(c, "channel", failingChannel);
    setField(c, "raf", spyRaf);

    assertDoesNotThrow(c::close);
    verify(spyRaf).close();
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

  @Test
  void tryWithResourcesClosesAutomatically() throws Exception {
    final File f = tempDir.resolve("try-with-resources").toFile();
    try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
      raf.write(new byte[]{10, 20, 30});
    }

    final RandomAccessFileChannel c = new RandomAccessFileChannel();
    c.open(f);
    assertTrue(c.isOpen());
    // Closeable contract: close via try-with-resources helper
    closeAndVerify(c);
    assertFalse(c.isOpen(), "should be closed after try-with-resources");
    assertDoesNotThrow(c::close, "double close should be safe");
  }

  private static void closeAndVerify(Closeable closeable) throws IOException {
    try (Closeable c = closeable) {
      assertNotNull(c);
    }
  }

  private static void setField(Object target, String name, Object value)
      throws ReflectiveOperationException {
    final Field f = RandomAccessFileChannel.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}

