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

package org.apache.hadoop.hdds.fs;

import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link SpaceUsageSource} implementations for testing.
 */
public final class MockSpaceUsageSource {

  public static SpaceUsageSource unlimited() {
    return fixed(Long.MAX_VALUE, Long.MAX_VALUE);
  }

  public static SpaceUsageSource fixed(long capacity, long available) {
    return fixed(capacity, available, capacity - available);
  }

  public static SpaceUsageSource fixed(long capacity, long available,
      long used) {
    return new SpaceUsageSource.Fixed(capacity, available, used);
  }

  /** @return {@code SpaceUsageSource} with fixed capacity and dynamic usage */
  public static SpaceUsageSource of(long capacity, AtomicLong used) {
    return new SpaceUsageSource() {
      @Override
      public long getUsedSpace() {
        return used.get();
      }

      @Override
      public long getCapacity() {
        return capacity;
      }

      @Override
      public long getAvailable() {
        return getCapacity() - getUsedSpace();
      }
    };
  }

  private MockSpaceUsageSource() {
    throw new UnsupportedOperationException("no instances");
  }

}
