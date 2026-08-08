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

import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.MAX_PAGE_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.MAX_SHARD_SIZE;

/**
 * Per-job batch sizing for container ID export.
 * These values are always used together when reading container IDs from SCM and writing them into the export TAR:
 * {@code pageSize} container IDs returned per {@code ContainerManager} listing query.
 * {@code shardSize} - container IDs written into each shard text file before that file is appended to the TAR.
 * {@code maxRows} - upper bound on total exported container IDs for the job; {@code 0} means no limit.
 * <p>
 * A {@code pageSize} or {@code shardSize} of {@code 0} in a submit request means use the
 * manager defaults from {@link ExportLimits}.
 */
final class ExportSizing {

  private final long maxRows;
  private final int pageSize;
  private final int shardSize;

  ExportSizing(long maxRows, int pageSize, int shardSize) {
    this.maxRows = maxRows;
    this.pageSize = pageSize;
    this.shardSize = shardSize;
  }

  static void validate(long maxRows, int pageSize, int shardSize) {
    if (maxRows < 0) {
      throw new IllegalArgumentException("maxRows must be non-negative: " + maxRows);
    }
    if (pageSize < 0 || pageSize > MAX_PAGE_SIZE) {
      throw new IllegalArgumentException("pageSize must be between 0 and " + MAX_PAGE_SIZE + ": " + pageSize);
    }
    if (shardSize < 0 || shardSize > MAX_SHARD_SIZE) {
      throw new IllegalArgumentException("shardSize must be between 0 and " + MAX_SHARD_SIZE + ": " + shardSize);
    }
  }

  static ExportSizing resolve(long maxRows, int pageSize, int shardSize, int defaultPageSize,
      int defaultShardSize) {
    return new ExportSizing(maxRows,
        pageSize > 0 ? pageSize : defaultPageSize,
        shardSize > 0 ? shardSize : defaultShardSize);
  }

  long getMaxRows() {
    return maxRows;
  }

  int getPageSize() {
    return pageSize;
  }

  int getShardSize() {
    return shardSize;
  }
}
