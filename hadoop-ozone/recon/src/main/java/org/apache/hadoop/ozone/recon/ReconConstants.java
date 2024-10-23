/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon;

/**
 * Recon Server constants file.
 */
public final class ReconConstants {

  private ReconConstants() {
    // Never Constructed
  }

  public static final String RECON_CONTAINER_KEY_DB = "recon-container-key.db";

  public static final String CONTAINER_COUNT_KEY = "containerCount";

  public static final String RECON_OM_SNAPSHOT_DB = "om.snapshot.db";

  public static final String RECON_SCM_SNAPSHOT_DB = "scm.snapshot.db";

  // By default, limit the number of results returned

  /**
   * The maximum number of top disk usage records to return in a /du response.
   */
  public static final int DISK_USAGE_TOP_RECORDS_LIMIT = 30;
  public static final String DEFAULT_OPEN_KEY_INCLUDE_NON_FSO = "false";
  public static final String DEFAULT_OPEN_KEY_INCLUDE_FSO = "false";
  public static final String DEFAULT_FETCH_COUNT = "1000";
  public static final String DEFAULT_KEY_SIZE = "0";
  public static final String DEFAULT_BATCH_NUMBER = "1";
  public static final String RECON_QUERY_BATCH_PARAM = "batchNum";
  public static final String RECON_QUERY_PREVKEY = "prevKey";
  public static final String RECON_QUERY_START_PREFIX = "startPrefix";
  public static final String RECON_OPEN_KEY_INCLUDE_NON_FSO = "includeNonFso";
  public static final String RECON_OPEN_KEY_INCLUDE_FSO = "includeFso";
  public static final String RECON_OM_INSIGHTS_DEFAULT_START_PREFIX = "/";
  public static final String RECON_OM_INSIGHTS_DEFAULT_SEARCH_LIMIT = "1000";
  public static final String RECON_OM_INSIGHTS_DEFAULT_SEARCH_PREV_KEY = "";
  public static final String RECON_QUERY_FILTER = "missingIn";
  public static final String PREV_CONTAINER_ID_DEFAULT_VALUE = "0";
  public static final String PREV_DELETED_BLOCKS_TRANSACTION_ID_DEFAULT_VALUE = "0";
  // Only include containers that are missing in OM by default
  public static final String DEFAULT_FILTER_FOR_MISSING_CONTAINERS = "SCM";
  public static final String RECON_QUERY_LIMIT = "limit";
  public static final String RECON_QUERY_VOLUME = "volume";
  public static final String RECON_QUERY_BUCKET = "bucket";
  public static final String RECON_QUERY_FILE_SIZE = "fileSize";
  public static final String RECON_QUERY_CONTAINER_SIZE = "containerSize";
  public static final String RECON_ENTITY_PATH = "path";
  public static final String RECON_ENTITY_TYPE = "entityType";
  public static final String RECON_ACCESS_METADATA_START_DATE = "startDate";
  public static final String CONTAINER_COUNT = "CONTAINER_COUNT";
  public static final String TOTAL_KEYS = "TOTAL_KEYS";
  public static final String TOTAL_USED_BYTES = "TOTAL_USED_BYTES";

  // 1125899906842624L = 1PB
  public static final long MAX_FILE_SIZE_UPPER_BOUND = 1125899906842624L;
  // 1024 = 1KB
  public static final long MIN_FILE_SIZE_UPPER_BOUND = 1024L;
  // 41 bins
  public static final int NUM_OF_FILE_SIZE_BINS = (int) Math.ceil(Math.log(
      (double) MAX_FILE_SIZE_UPPER_BOUND / MIN_FILE_SIZE_UPPER_BOUND) /
      Math.log(2)) + 1;


  // 1125899906842624L = 1PB
  public static final long MAX_CONTAINER_SIZE_UPPER_BOUND = 1125899906842624L;
  // 536870912L = 512MB
  public static final long MIN_CONTAINER_SIZE_UPPER_BOUND = 536870912L;
  // 14 bins
  public static final int NUM_OF_CONTAINER_SIZE_BINS = (int) Math.ceil(Math.log(
      (double) MAX_CONTAINER_SIZE_UPPER_BOUND /
          MIN_CONTAINER_SIZE_UPPER_BOUND) /
      Math.log(2)) + 1;
}
