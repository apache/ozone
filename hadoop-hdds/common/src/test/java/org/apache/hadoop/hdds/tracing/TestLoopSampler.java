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

package org.apache.hadoop.hdds.tracing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LoopSampler}: invalid ratios, fixed outcomes at 0 and 1 (and above 1),
 * and approximate behavior at 50%.
 */
public class TestLoopSampler {

  /**
   * negative sampling ration must throw error.
   */
  @Test
  public void negativeRatioThrows() {
    assertThrows(IllegalArgumentException.class, () -> new LoopSampler(-0.01));
  }

  /**
   * Test to check if given a ratio of zero for a span, that it should never be sampled.
   */
  @Test
  public void zeroNeverSamples() {
    LoopSampler s = new LoopSampler(0.0);
    for (int i = 0; i < 50; i++) {
      assertFalse(s.shouldSample());
    }
  }

  /**
   * Ration of one , indicates that span should always be sampled.
   */
  @Test
  public void oneAlwaysSamples() {
    LoopSampler s = new LoopSampler(1.0);
    for (int i = 0; i < 50; i++) {
      assertTrue(s.shouldSample());
    }
  }

  /**
   * Ration above one is taken as , every span should be sampled for that value.
   */
  @Test
  public void aboveOneIsCappedToAlwaysSample() {
    LoopSampler s = new LoopSampler(2.0);
    for (int i = 0; i < 50; i++) {
      assertTrue(s.shouldSample());
    }
  }

  /**
   * Test to check if ratio of half gives approximately half spans as selected.
   */
  @Test
  public void halfSamplesStatistically() {
    LoopSampler s = new LoopSampler(0.5);
    int hits = 0;
    int n = 20_000;
    for (int i = 0; i < n; i++) {
      if (s.shouldSample()) {
        hits++;
      }
    }
    assertTrue(hits > n * 0.45 && hits < n * 0.55,
        "expected ~50% samples, got " + hits + " / " + n);
  }
}
