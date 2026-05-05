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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Test for MappedBufferManager.
 */
public class TestMappedBufferManager {

  @Test
  public void testComputeIfAbsent() {
    MappedBufferManager manager = new MappedBufferManager(100);
    String file = "/CID-fd49f4a7-670d-43c5-a177-8ac03aafceb2/current/containerDir0/2/chunks/113750153625600065.block";
    long position = 0;
    int size = 1024;
    ByteBuffer buffer1 = ByteBuffer.allocate(size);
    ByteBuffer buffer2 = ByteBuffer.allocate(size + 1);
    ByteBuffer byteBuffer1 = manager.computeIfAbsent(file, position, size, () -> buffer1);
    assertEquals(buffer1, byteBuffer1);
    // buffer should be reused
    String file2 = "/CID-fd49f4a7-670d-43c5-a177-8ac03aafceb2/current/containerDir0/2/chunks/113750153625600065.block";
    ByteBuffer byteBuffer2 = manager.computeIfAbsent(file2, position, size, () -> buffer2);
    assertEquals(buffer1, byteBuffer2);
  }
}
