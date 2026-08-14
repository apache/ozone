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

/**
 * Shared limits and defaults for container ID export.
 *
 * <p>A <b>page</b> is the batch of container IDs returned by one {@code ContainerManager} listing
 * call during export. A <b>shard</b> is one text file in the export archive; export rotates to a
 * new shard after {@code shardSize} container IDs have been written.
 *
 * <p>{@code DEFAULT_*} values apply when a submit request passes {@code 0} for that setting.
 * {@code MAX_*} values bound per-job overrides.
 */
public final class ExportLimits {

  public static final String EXPORT_SUBDIR = "exports";

  // Default container IDs fetched per {@code ContainerManager} listing call.
  public static final int DEFAULT_PAGE_SIZE = 100_000;
  // Default container IDs written into each shard text file.
  public static final int DEFAULT_SHARD_SIZE = 500_000;
  // Maximum allowed {@code pageSize} override for a single export job.
  public static final int MAX_PAGE_SIZE = 1_000_000;
  // Maximum allowed {@code shardSize} override for a single export job.
  public static final int MAX_SHARD_SIZE = 5_000_000;

  private ExportLimits() {
  }
}
