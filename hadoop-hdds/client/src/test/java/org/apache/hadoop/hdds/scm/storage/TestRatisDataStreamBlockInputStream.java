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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RatisDataStreamBlockInputStream}.
 */
class TestRatisDataStreamBlockInputStream {
  private static final long ONE_GB = 1L << 30;
  private static final long READ_WINDOW = 256L << 20;
  private static final long PRE_READ = 32L << 20;

  @Test
  void readLengthUsesLargeWindowOnlyAfterSequentialStream() {
    final int smallRead = 4 << 10;

    assertEquals(smallRead,
        RatisDataStreamBlockInputStream.computeReadLengthForTesting(
            ONE_GB, 0, smallRead, true, false, PRE_READ, READ_WINDOW));
    assertEquals(READ_WINDOW,
        RatisDataStreamBlockInputStream.computeReadLengthForTesting(
            ONE_GB, PRE_READ + smallRead, smallRead, true, true, PRE_READ,
            READ_WINDOW));
    assertEquals(smallRead,
        RatisDataStreamBlockInputStream.computeReadLengthForTesting(
            ONE_GB, 0, smallRead, false, true, PRE_READ, READ_WINDOW));
    assertEquals(1024,
        RatisDataStreamBlockInputStream.computeReadLengthForTesting(
            ONE_GB, ONE_GB - 1024, smallRead, true, true, PRE_READ,
            READ_WINDOW));
  }
}
