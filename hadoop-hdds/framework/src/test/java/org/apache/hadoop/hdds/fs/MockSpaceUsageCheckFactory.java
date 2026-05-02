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

import java.io.File;
import java.time.Duration;

/**
 * {@link SpaceUsageCheckFactory} implementations for testing.
 */
public final class MockSpaceUsageCheckFactory {

  public static final SpaceUsageCheckFactory NONE = new None();

  /**
   * Creates a factory that uses the specified parameters for all directories.
   */
  public static SpaceUsageCheckFactory of(SpaceUsageSource source,
      Duration refresh, SpaceUsagePersistence persistence) {
    return dir -> new SpaceUsageCheckParams(dir, source, refresh, persistence);
  }

  /**
   * An implementation that never checks space usage but reports basically
   * unlimited free space.  Neither does it persist space usage info.
   */
  public static class None implements SpaceUsageCheckFactory {
    @Override
    public SpaceUsageCheckParams paramsFor(File dir) {
      return new SpaceUsageCheckParams(dir,
          MockSpaceUsageSource.unlimited(),
          Duration.ZERO,
          SpaceUsagePersistence.None.INSTANCE
      );
    }
  }

  /**
   * An implementation that never checks space usage but reports basically
   * 512G free space.  Neither does it persist space usage info.
   */
  public static class HalfTera implements SpaceUsageCheckFactory {
    @Override
    public SpaceUsageCheckParams paramsFor(File dir) {
      return new SpaceUsageCheckParams(dir,
          MockSpaceUsageSource.fixed(512L * 1024 * 1024 * 1024,
              512L * 1024 * 1024 * 1024),
          Duration.ZERO,
          SpaceUsagePersistence.None.INSTANCE
      );
    }
  }

  private MockSpaceUsageCheckFactory() {
    throw new UnsupportedOperationException("no instances");
  }

}
