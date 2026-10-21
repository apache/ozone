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

package org.apache.hadoop.hdds.scm.container.export;

import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.MAX_BATCH_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.MAX_PART_SIZE;

/**
 * Per-job batch sizing for container ID export.
 * These values are always used together when reading container IDs from SCM and writing them into the export TAR:
 * {@code batchSize} container IDs returned per {@code ContainerManager} listing query.
 * {@code partSize} - container IDs written into each part text file before that file is appended to the TAR.
 * {@code maxRows} - upper bound on total exported container IDs for the job; {@code 0} means no limit.
 * <p>
 * A {@code batchSize} or {@code partSize} of {@code 0} in a submit request means use the
 * manager defaults. The {@code MAX_*} values in {@link ExportLimits} bound the request values.
 */
final class ExportSizing {

  private final long maxRows;
  private final int batchSize;
  private final int partSize;

  ExportSizing(long maxRows, int batchSize, int partSize, int defaultBatchSize, int defaultPartSize) {
    if (maxRows < 0) {
      throw new IllegalArgumentException("maxRows must be non-negative: " + maxRows);
    }
    this.maxRows = maxRows;
    this.batchSize = resolve("batchSize", batchSize, defaultBatchSize, MAX_BATCH_SIZE);
    this.partSize = resolve("partSize", partSize, defaultPartSize, MAX_PART_SIZE);
  }

  /** Checks the default and the requested value ({@code 0} selects the default), and returns the one to use. */
  private static int resolve(String name, int requested, int defaultValue, int max) {
    if (defaultValue <= 0 || defaultValue > max) {
      throw new IllegalArgumentException("default " + name + " must be between 1 and " + max + ": " + defaultValue);
    }
    if (requested < 0 || requested > max) {
      throw new IllegalArgumentException(name + " must be between 0 and " + max + ": " + requested);
    }
    return requested > 0 ? requested : defaultValue;
  }

  long getMaxRows() {
    return maxRows;
  }

  int getBatchSize() {
    return batchSize;
  }

  int getPartSize() {
    return partSize;
  }
}
