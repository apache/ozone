/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.ozone.common.ChunkBuffer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for buffer pool.
 */
public class TestBufferPool {

  @Test
  public void releaseAndReallocate() {
    BufferPool pool = new BufferPool(1024, 8);
    ChunkBuffer cb1 = pool.allocateBuffer(0);
    ChunkBuffer cb2 = pool.allocateBuffer(0);
    ChunkBuffer cb3 = pool.allocateBuffer(0);

    pool.releaseBuffer(cb1);

    //current state cb2, -> cb3, cb1
    final ChunkBuffer allocated = pool.allocateBuffer(0);
    Assert.assertEquals(3, pool.getSize());
    Assert.assertEquals(cb1, allocated);
  }

}