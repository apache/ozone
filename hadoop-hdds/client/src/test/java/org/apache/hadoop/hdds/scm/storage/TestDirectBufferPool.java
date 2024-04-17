/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * UNIT test for DirectBufferPool.
 */
public class TestDirectBufferPool {
  private final DirectBufferPool pool = new DirectBufferPool();
  
  @Test
  public void testDirectBufferAllocation() {
    ByteBuffer a = pool.allocateBuffer(100);
    assertEquals(100, a.capacity());
    assertEquals(100, a.remaining());
    pool.releaseBuffer(a);
    
    // Getting a new buffer should return the same one
    ByteBuffer b = pool.allocateBuffer(100);
    assertSame(a, b);
    
    // Getting a new buffer before returning "B" should
    // not return the same one
    ByteBuffer c = pool.allocateBuffer(100);
    assertNotSame(b, c);
    pool.releaseBuffer(b);
    pool.releaseBuffer(c);
  }
  
  @Test
  public void testBuffersAreReset() {
    ByteBuffer a = pool.allocateBuffer(100);
    a.putInt(0xdeadbeef);
    assertEquals(96, a.remaining());
    pool.releaseBuffer(a);

    // Even though we return the same buffer,
    // its position should be reset to 0
    ByteBuffer b = pool.allocateBuffer(100);
    assertSame(a, b);
    assertEquals(100, a.remaining());
    pool.releaseBuffer(b);
  }
  
  @Test
  public void testWeakRefClearing() {
    // Allocate and return 10 buffers.
    List<ByteBuffer> bufs = Lists.newLinkedList();
    for (int i = 0; i < 10; i++) {
      ByteBuffer buf = pool.allocateBuffer(100);
      bufs.add(buf);
    }
    
    for (ByteBuffer buf : bufs) {
      pool.releaseBuffer(buf);
    }

    assertEquals(10, pool.countBuffersOfSize(100));

    // Clear out any references to the buffers, and force
    // GC. Weak refs should get cleared.
    bufs.clear();
    bufs = null;
    for (int i = 0; i < 3; i++) {
      System.gc();
    }

    ByteBuffer buf = pool.allocateBuffer(100);
    // the act of getting a buffer should clear all the nulled
    // references from the pool.
    assertEquals(0, pool.countBuffersOfSize(100));
    pool.releaseBuffer(buf);
  }
}
