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

package org.apache.hadoop.hdds.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NettyMetrics}, in particular the public-API replacements for
 * the direct memory gauges.
 */
class TestNettyMetrics {

  private static final long MAX_HEAP = 8L << 30;

  @Test
  void resolveUsesNettyProperty() {
    assertEquals(4096L, NettyMetrics.resolveMaxDirectMemory(
        4096L, Collections.emptyList(), MAX_HEAP));
    // The property takes precedence over the JVM flag, even when it is 0.
    assertEquals(0L, NettyMetrics.resolveMaxDirectMemory(
        0L, Arrays.asList("-XX:MaxDirectMemorySize=1g"), MAX_HEAP));
  }

  @Test
  void resolveUsesMaxDirectMemorySizeFlag() {
    assertEquals(512L << 20, NettyMetrics.resolveMaxDirectMemory(
        -1L, Arrays.asList("-XX:MaxDirectMemorySize=512m"), MAX_HEAP));
    assertEquals(1L << 30, NettyMetrics.resolveMaxDirectMemory(
        -1L, Arrays.asList("-Xmx4g", "-XX:MaxDirectMemorySize=1g"), MAX_HEAP));
    assertEquals(1073741824L, NettyMetrics.resolveMaxDirectMemory(
        -1L, Arrays.asList("-XX:MaxDirectMemorySize=1073741824"), MAX_HEAP));
  }

  @Test
  void resolveFallsBackToMaxHeap() {
    assertEquals(MAX_HEAP, NettyMetrics.resolveMaxDirectMemory(
        -1L, Collections.emptyList(), MAX_HEAP));
    assertEquals(MAX_HEAP, NettyMetrics.resolveMaxDirectMemory(
        -1L, Arrays.asList("-Xmx8g"), MAX_HEAP));
    // 0 is not a positive limit, so fall back.
    assertEquals(MAX_HEAP, NettyMetrics.resolveMaxDirectMemory(
        -1L, Arrays.asList("-XX:MaxDirectMemorySize=0"), MAX_HEAP));
    // Malformed value, so fall back.
    assertEquals(MAX_HEAP, NettyMetrics.resolveMaxDirectMemory(
        -1L, Arrays.asList("-XX:MaxDirectMemorySize=bogus"), MAX_HEAP));
  }

  @Test
  void parseSizeVariants() {
    assertEquals(1L << 10, NettyMetrics.parseSize("1k"));
    assertEquals(1L << 20, NettyMetrics.parseSize("1m"));
    assertEquals(1L << 30, NettyMetrics.parseSize("1G"));
    assertEquals(1L << 40, NettyMetrics.parseSize("1t"));
    assertEquals(2048L, NettyMetrics.parseSize("2048"));
    assertEquals(-1L, NettyMetrics.parseSize(""));
    assertEquals(-1L, NettyMetrics.parseSize("abc"));
    assertEquals(-1L, NettyMetrics.parseSize("m"));
  }

  @Test
  void usedDirectMemoryDoesNotThrow() {
    assertThat(NettyMetrics.usedDirectMemory()).isGreaterThanOrEqualTo(-1L);
  }

  @Test
  void maxDirectMemoryIsPositive() {
    assertThat(NettyMetrics.maxDirectMemory()).isGreaterThan(0L);
  }
}
