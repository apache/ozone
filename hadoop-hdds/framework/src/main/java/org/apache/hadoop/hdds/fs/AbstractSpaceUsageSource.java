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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.LongSupplier;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

/**
 * Convenience parent class for {@code SpaceUsageSource} implementations.
 */
public abstract class AbstractSpaceUsageSource implements SpaceUsageSource {

  private final File file;
  private final String path;

  /**
   * @param file the path to check disk usage in
   */
  protected AbstractSpaceUsageSource(File file) {
    this.file = file;
    try {
      this.path = file.getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Measures execution time of {@code supplier#getAsLong} and logs it via the
   * given {@code logger}.
   * @return the same value as returned by {@code supplier#getAsLong}
   */
  protected static long time(LongSupplier supplier, Logger logger) {
    long start = Time.monotonicNow();

    long result = supplier.getAsLong();

    long end = Time.monotonicNow();
    long elapsed = end - start;
    logger.debug("Completed check in {} ms, result: {}", elapsed, result);

    return result;
  }

  protected String getPath() {
    return path;
  }

  protected File getFile() {
    return file;
  }

  @Override
  public String toString() {
    return path;
  }

  /**
   * Get available space, excluding system reserved space.
   * See {@link File#getUsableSpace()} and {@link File#getFreeSpace()}.
   * @return available space for data
   */
  @Override
  public long getAvailable() {
    return file.getUsableSpace();
  }

  @Override
  public long getCapacity() {
    return file.getTotalSpace();
  }
}
