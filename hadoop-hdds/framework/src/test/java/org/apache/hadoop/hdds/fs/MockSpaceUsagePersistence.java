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

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link SpaceUsagePersistence} implementations for testing.
 */
public final class MockSpaceUsagePersistence {

  public static SpaceUsagePersistence inMemory(AtomicLong target) {
    return new Memory(target);
  }

  private static class Memory implements SpaceUsagePersistence {

    private final AtomicLong target;

    Memory(AtomicLong target) {
      this.target = target;
    }

    @Override
    public OptionalLong load() {
      return OptionalLong.of(target.get());
    }

    @Override
    public void save(SpaceUsageSource source) {
      target.set(source.getUsedSpace());
    }
  }

  private MockSpaceUsagePersistence() {
    throw new UnsupportedOperationException("no instances");
  }

}
