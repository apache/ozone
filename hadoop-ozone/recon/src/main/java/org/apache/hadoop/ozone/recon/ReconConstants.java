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

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_SUFFIX;
import static org.apache.hadoop.ozone.OzoneConsts.PIPELINE_DB_SUFFIX;

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

  public static final String CONTAINER_KEY_TABLE = "containerKeyTable";

  public static final String CONTAINER_KEY_COUNT_TABLE =
      "containerKeyCountTable";

  // By default, limit the number of results returned
  public static final String DEFAULT_FETCH_COUNT = "1000";
  public static final String DEFAULT_BATCH_NUMBER = "1";
  public static final String RECON_QUERY_BATCH_PARAM = "batchNum";
  public static final String RECON_QUERY_PREVKEY = "prevKey";
  public static final String PREV_CONTAINER_ID_DEFAULT_VALUE = "0";
  public static final String RECON_QUERY_LIMIT = "limit";
  public static final String RECON_QUERY_VOLUME = "volume";
  public static final String RECON_QUERY_BUCKET = "bucket";
  public static final String RECON_QUERY_FILE_SIZE = "fileSize";

  public static final String RECON_SCM_CONTAINER_DB =
      "recon-" + CONTAINER_DB_SUFFIX;
  public static final String RECON_SCM_PIPELINE_DB = "recon-"
      + PIPELINE_DB_SUFFIX;
  public static final String RECON_SCM_NODE_DB =
      "recon-node.db";
}
