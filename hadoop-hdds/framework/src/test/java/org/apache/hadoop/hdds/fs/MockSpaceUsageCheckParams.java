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
 * {@link SpaceUsageCheckParams} builder for testing.
 */
public final class MockSpaceUsageCheckParams {

  public static Builder newBuilder(File dir) {
    return new Builder(dir);
  }

  /**
   * Builder of {@link SpaceUsageCheckParams} for testing.
   */
  public static final class Builder {

    private final File dir;
    private SpaceUsageSource source = MockSpaceUsageSource.unlimited();
    private Duration refresh = Duration.ZERO;
    private SpaceUsagePersistence persistence =
        SpaceUsagePersistence.None.INSTANCE;

    private Builder(File dir) {
      this.dir = dir;
    }

    public Builder withSource(SpaceUsageSource newSource) {
      this.source = newSource;
      return this;
    }

    public Builder withRefresh(Duration newRefresh) {
      this.refresh = newRefresh;
      return this;
    }

    public Builder withPersistence(SpaceUsagePersistence newPersistence) {
      this.persistence = newPersistence;
      return this;
    }

    public SpaceUsageCheckParams build() {
      return new SpaceUsageCheckParams(dir, source, refresh, persistence);
    }
  }

  private MockSpaceUsageCheckParams() {
    throw new UnsupportedOperationException("no instances");
  }

}
