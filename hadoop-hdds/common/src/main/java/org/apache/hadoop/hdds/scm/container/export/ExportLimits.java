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
 */
public final class ExportLimits {

  public static final String EXPORT_SUBDIR = "exports";

  public static final int DEFAULT_PAGE_SIZE = 100_000;
  public static final int DEFAULT_SHARD_SIZE = 500_000;
  public static final int MAX_PAGE_SIZE = 1_000_000;
  public static final int MAX_SHARD_SIZE = 5_000_000;

  private ExportLimits() {
  }
}
