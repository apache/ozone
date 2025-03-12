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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Saves and loads space usage information to/from a file.
 */
public class SaveSpaceUsageToFile implements SpaceUsagePersistence {

  private static final Logger LOG =
      LoggerFactory.getLogger(SaveSpaceUsageToFile.class);

  private final File file;
  private final Duration expiry;

  public SaveSpaceUsageToFile(File file, Duration expiry) {
    this.file = file;
    this.expiry = expiry;

    Preconditions.checkArgument(file != null, "file == null");
    Preconditions.checkArgument(expiry != null, "expiry == null");
    Preconditions.checkArgument(!expiry.isNegative() && !expiry.isZero(),
        "invalid expiry: %s", expiry);
  }

  /**
   * Read in the cached DU value and return it if it is newer than the given
   * expiry. Slight imprecision is not critical
   * and skipping DU can significantly shorten the startup time.
   * If the cached value is not available or too old, empty {@code OptionalLong}
   * is returned.
   */
  @Override
  public OptionalLong load() {
    try (Scanner sc = new Scanner(file, UTF_8.name())) {
      // Get the recorded value from the file.
      if (!sc.hasNextLong()) {
        LOG.info("Cached usage info in {} has no value", file);
        return OptionalLong.empty();
      }
      long cachedValue = sc.nextLong();

      // Get the recorded time from the file.
      if (!sc.hasNextLong()) {
        LOG.info("Cached usage info in {} has no time", file);
        return OptionalLong.empty();
      }

      Instant time = Instant.ofEpochMilli(sc.nextLong());
      if (isExpired(time)) {
        LOG.info("Cached usage info in {} is expired: {} ", file, time);
        return OptionalLong.empty();
      }

      LOG.info("Cached usage info found in {}: {} at {}", file, cachedValue,
          time);

      return OptionalLong.of(cachedValue);
    } catch (FileNotFoundException e) {
      LOG.info("Cached usage info file {} not found", file);
      return OptionalLong.empty();
    }
  }

  /**
   * Write the current usage info to the cache file.
   */
  @Override
  public void save(SpaceUsageSource source) {
    try {
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      LOG.warn("Failed to delete old usage file {}: {}.", file, e.getMessage());
    }

    long used = source.getUsedSpace();
    if (used > 0) {
      Instant now = Instant.now();
      try (OutputStream fileOutput = Files.newOutputStream(file.toPath());
           Writer out = new OutputStreamWriter(fileOutput, UTF_8)) {
        // time is written last, so that truncated writes won't be valid.
        out.write(used + " " + now.toEpochMilli());
        out.flush();
      } catch (IOException e) {
        // If write failed, the volume might be bad. Since the cache file is
        // not critical, log the error and continue.
        LOG.warn("Failed to write usage to {}", file, e);
      }
    }
  }

  private boolean isExpired(Instant time) {
    return time.plus(expiry).isBefore(Instant.now());
  }
}
