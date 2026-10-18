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

import java.util.concurrent.ThreadLocalRandom;

/**
 * Probability-based span sampler that samples spans independently.
 * Uses ThreadLocalRandom for probability decisions.
 */
public final class LoopSampler {
  private final double probability;

  public LoopSampler(double ratio) {
    if (ratio < 0) {
      throw new IllegalArgumentException("Sampling ratio cannot be negative: " + ratio);
    }
    // Cap at 1.0 to prevent logic errors
    this.probability = Math.min(ratio, 1.0);
  }

  public boolean shouldSample() {
    if (probability <= 0) {
      return false;
    }
    if (probability >= 1.0) {
      return true;
    }
    return ThreadLocalRandom.current().nextDouble() < probability;
  }
}
