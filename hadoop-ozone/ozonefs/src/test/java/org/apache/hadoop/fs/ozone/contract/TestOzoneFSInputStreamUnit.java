/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone.contract;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ozone.OzoneFSInputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

// TODO rename to TestOzoneFSInputStream after HDDS-2785
/**
 * Tests for {@link OzoneFSInputStream}.
 */
public class TestOzoneFSInputStreamUnit {

  @Test
  public void readToByteBuffer() throws IOException {
    for (int streamLength = 1; streamLength <= 10; streamLength++) {
      for (int bufferCapacity = 0; bufferCapacity <= 10; bufferCapacity++) {
        testReadToByteBuffer(streamLength, bufferCapacity, 0);
        if (bufferCapacity > 1) {
          testReadToByteBuffer(streamLength, bufferCapacity, 1);
          if (bufferCapacity > 2) {
            testReadToByteBuffer(streamLength, bufferCapacity,
                bufferCapacity - 1);
          }
        }
        testReadToByteBuffer(streamLength, bufferCapacity, bufferCapacity);
      }
    }
  }

  private static void testReadToByteBuffer(int streamLength, int bufferCapacity,
      int bufferPosition) throws IOException {
    final byte[] source = RandomUtils.nextBytes(streamLength);
    final InputStream input = new ByteArrayInputStream(source);
    final OzoneFSInputStream subject = createStream(input);

    final int expectedReadLength = Math.min(bufferCapacity - bufferPosition,
        input.available());
    final byte[] expectedContent = Arrays.copyOfRange(source, 0,
        expectedReadLength);

    final ByteBuffer buf = ByteBuffer.allocate(bufferCapacity);
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
    final OzoneFSInputStream subject = emptyStream();
    final ByteBuffer buf = ByteBuffer.allocate(1);

    final int bytesRead = subject.read(buf);

    assertEquals(-1, bytesRead);
    assertEquals(0, buf.position());
  }

  private static OzoneFSInputStream createStream(InputStream input) {
    return new OzoneFSInputStream(input,
        new FileSystem.Statistics("test"));
  }

  private static OzoneFSInputStream emptyStream() {
    return createStream(new ByteArrayInputStream(new byte[0]));
  }

}
