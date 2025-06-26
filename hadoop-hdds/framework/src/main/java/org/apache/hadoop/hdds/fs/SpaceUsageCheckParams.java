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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * Parameters for performing disk space usage checks.  Bundles the source of the
 * information, refresh duration, a way to save/load the last available value.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SpaceUsageCheckParams {

  private final SpaceUsageSource source;
  private final Duration refresh;
  private final SpaceUsagePersistence persistence;
  private final String path;
  private final File dir;
  private Supplier<Long> containerUsedSpace = () -> 0L;

  /**
   * @param refresh The period of refreshing space usage information from
   *                {@code source}.  May be {@link Duration#ZERO} to skip
   *                periodic refresh, but cannot be negative.
   * @throws UncheckedIOException if canonical path for {@code dir} cannot be
   * resolved
   */
  public SpaceUsageCheckParams(File dir, SpaceUsageSource source,
      Duration refresh, SpaceUsagePersistence persistence) {

    checkArgument(dir != null, "dir == null");
    checkArgument(source != null, "source == null");
    checkArgument(refresh != null, "refresh == null");
    checkArgument(persistence != null, "persistence == null");
    checkArgument(!refresh.isNegative(), "refresh is negative");

    this.dir = dir;
    this.source = source;
    this.refresh = refresh;
    this.persistence = persistence;

    try {
      path = dir.getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public File getDir() {
    return dir;
  }

  public String getPath() {
    return path;
  }

  public SpaceUsageSource getSource() {
    return source;
  }

  public Duration getRefresh() {
    return refresh;
  }

  public SpaceUsagePersistence getPersistence() {
    return persistence;
  }

  public void setContainerUsedSpace(Supplier<Long> containerUsedSpace) {
    this.containerUsedSpace = containerUsedSpace;
  }

  public Supplier<Long> getContainerUsedSpace() {
    return containerUsedSpace;
  }
}
