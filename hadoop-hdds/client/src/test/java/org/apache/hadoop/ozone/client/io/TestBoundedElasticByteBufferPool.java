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

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for BoundedElasticByteBufferPool.
 */
public class TestBoundedElasticByteBufferPool {

  private static final int MB = 1024 * 1024;
  private static final long MAX_POOL_SIZE = 3L * MB; // 3MB

  @Test
  public void testLogicalTimestampOrdering() {
    // Pool with plenty of capacity
    BoundedElasticByteBufferPool pool = new BoundedElasticByteBufferPool(MAX_POOL_SIZE);
    int bufferSize = 5 * 1024; // 5KB

    // Create and add three distinct buffers of the same size
    ByteBuffer buffer1 = ByteBuffer.allocate(bufferSize);
    ByteBuffer buffer2 = ByteBuffer.allocate(bufferSize);
    ByteBuffer buffer3 = ByteBuffer.allocate(bufferSize);

    // Store their unique identity hash codes
    int hash1 = System.identityHashCode(buffer1);
    int hash2 = System.identityHashCode(buffer2);
    int hash3 = System.identityHashCode(buffer3);

    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    pool.putBuffer(buffer3);

    // The pool should now contain 15KB data
    Assertions.assertEquals(bufferSize * 3L, pool.getCurrentPoolSize());

    // Get the buffers back. They should come back in the same
    // order they were put in (FIFO).
    ByteBuffer retrieved1 = pool.getBuffer(false, bufferSize);
    ByteBuffer retrieved2 = pool.getBuffer(false, bufferSize);
    ByteBuffer retrieved3 = pool.getBuffer(false, bufferSize);

    // Verify we got the exact same buffer instances back in FIFO order
    Assertions.assertEquals(hash1, System.identityHashCode(retrieved1));
    Assertions.assertEquals(hash2, System.identityHashCode(retrieved2));
    Assertions.assertEquals(hash3, System.identityHashCode(retrieved3));

    // The pool should now be empty
    Assertions.assertEquals(0, pool.getCurrentPoolSize());
  }

  /**
   * Verifies the core feature: the pool stops caching buffers
   * once its maximum size is reached.
   */
  @Test
  public void testPoolBoundingLogic() {
    BoundedElasticByteBufferPool pool = new BoundedElasticByteBufferPool(MAX_POOL_SIZE);

    ByteBuffer buffer1 = ByteBuffer.allocate(2 * MB);
    ByteBuffer buffer2 = ByteBuffer.allocate(1 * MB);
    ByteBuffer buffer3 = ByteBuffer.allocate(3 * MB);

    int hash1 = System.identityHashCode(buffer1);
    int hash2 = System.identityHashCode(buffer2);
    int hash3 = System.identityHashCode(buffer3);

    // 1. Put buffer 1 (Pool size: 2MB, remaining: 1MB)
    pool.putBuffer(buffer1);
    Assertions.assertEquals(2 * MB, pool.getCurrentPoolSize());

    // 2. Put buffer 2 (Pool size: 2MB + 1MB = 3MB, remaining: 0)
    // The check is (current(2MB) + new(1MB)) > max(3MB), which is false.
    // So, the buffer IS added.
    pool.putBuffer(buffer2);
    Assertions.assertEquals(3 * MB, pool.getCurrentPoolSize());

    // 3. Put buffer 3 (Capacity 3MB)
    // The check is (current(3MB) + new(3MB)) > max(3MB), which is true.
    // This buffer should be REJECTED.
    pool.putBuffer(buffer3);
    // The pool size should NOT change.
    Assertions.assertEquals(3 * MB, pool.getCurrentPoolSize());

    // 4. Get buffers back
    ByteBuffer retrieved1 = pool.getBuffer(false, 2 * MB);
    ByteBuffer retrieved2 = pool.getBuffer(false, 1 * MB);

    // The pool should now be empty
    Assertions.assertEquals(0, pool.getCurrentPoolSize());

    // 5. Ask for a third buffer.
    // Since buffer3 was rejected, this should be a NEWLY allocated buffer.
    ByteBuffer retrieved3 = pool.getBuffer(false, 3 * MB);

    // Verify that we got the first two buffers from the pool
    Assertions.assertEquals(hash1, System.identityHashCode(retrieved1));
    Assertions.assertEquals(hash2, System.identityHashCode(retrieved2));

    // Verify that the third buffer is a NEW instance, not buffer3
    Assertions.assertNotEquals(hash3, System.identityHashCode(retrieved3));
  }
}
