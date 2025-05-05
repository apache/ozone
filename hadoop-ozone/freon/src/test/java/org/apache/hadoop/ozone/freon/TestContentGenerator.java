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

package org.apache.hadoop.ozone.freon;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ContentGenerator class of Freon.
 */
public class TestContentGenerator {

  @Test
  public void writeWrite() throws IOException {
    ContentGenerator generator = new ContentGenerator(1024, 1024);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    generator.write(output);
    assertArrayEquals(generator.getBuffer(), output.toByteArray());
  }

  @Test
  public void writeWithSmallerBuffers() throws IOException {
    ContentGenerator generator = new ContentGenerator(10000, 1024, 3);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    generator.write(baos);

    assertEquals(10000, baos.toByteArray().length);
  }

  @Test
  public void writeWithByteLevelWrite() throws IOException {
    ContentGenerator generator = new ContentGenerator(1024, 1024, 1);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    generator.write(output);
    assertArrayEquals(generator.getBuffer(), output.toByteArray());
  }

  @Test
  public void writeWithSmallBuffer() throws IOException {
    ContentGenerator generator = new ContentGenerator(1024, 1024, 10);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    generator.write(output);
    assertArrayEquals(generator.getBuffer(), output.toByteArray());
  }

  @Test
  public void writeWithDistinctSizes() throws IOException {
    ContentGenerator generator = new ContentGenerator(20, 8, 3);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    generator.write(output);

    byte[] expected = new byte[20];
    byte[] buffer = generator.getBuffer();
    System.arraycopy(buffer, 0, expected, 0, buffer.length);
    System.arraycopy(buffer, 0, expected, 8, buffer.length);
    System.arraycopy(buffer, 0, expected, 16, 4);
    assertArrayEquals(expected, output.toByteArray());
  }

  @Test
  public void writeWithHsync() throws IOException {
    class SyncableByteArrayOutputStream extends ByteArrayOutputStream
        implements Syncable, StreamCapabilities {
      @Override
      public void hflush() throws IOException {
      }

      @Override
      public void hsync() throws IOException {
      }

      @Override
      public boolean hasCapability(String capability) {
        return true;
      }
    }

    ContentGenerator generator = new ContentGenerator(20, 8, 3,
        ContentGenerator.SyncOptions.HSYNC);

    SyncableByteArrayOutputStream syncable =
        new SyncableByteArrayOutputStream();
    SyncableByteArrayOutputStream spySyncable = spy(syncable);

    generator.write(spySyncable);
    verify(spySyncable, times(8)).hsync();

    generator = new ContentGenerator(20, 8, 3);
    spySyncable = spy(syncable);
    generator.write(spySyncable);
    verify(spySyncable, times(0)).hsync();
  }
}
