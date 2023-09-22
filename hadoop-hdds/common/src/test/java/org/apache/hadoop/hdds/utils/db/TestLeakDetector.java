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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test {@link CodecBuffer.LeakDetector}.
 */
public final class TestLeakDetector {
  @Test
  public void test() throws Exception {
    CodecBuffer.enableLeakDetection();
    // allocate a buffer and then release it.
    CodecBuffer.allocateHeap(2).release();
    // no leak at this point.
    CodecTestUtil.gc();

    // allocate a buffer but NOT release it.
    CodecBuffer.allocateHeap(3);
    // It should detect a buffer leak.
    final AssertionError e = Assertions.assertThrows(
        AssertionError.class, CodecTestUtil::gc);
    e.printStackTrace(System.out);
    Assertions.assertTrue(e.getMessage().startsWith("Found 1 leak"));
  }
}
