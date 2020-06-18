/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.impl;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_SCRUB_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_SCRUB_ENABLED_DEFAULT;

/**
 * Select an appropriate ChunkManager implementation as per config setting.
 */
public final class ChunkManagerFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkManagerFactory.class);

  private ChunkManagerFactory() {
  }

  /**
   * Create a chunk manager.
   * @param conf     Configuration
   * @param manager  This parameter will be used only for read data of
   *                 FILE_PER_CHUNK layout file. Can be null for other cases.
   * @return
   */
  public static ChunkManager createChunkManager(ConfigurationSource conf,
      BlockManager manager) {
    boolean sync =
        conf.getBoolean(OzoneConfigKeys.DFS_CONTAINER_CHUNK_WRITE_SYNC_KEY,
            OzoneConfigKeys.DFS_CONTAINER_CHUNK_WRITE_SYNC_DEFAULT);

    boolean persist = conf.getBoolean(HDDS_CONTAINER_PERSISTDATA,
        HDDS_CONTAINER_PERSISTDATA_DEFAULT);

    if (!persist) {
      boolean scrubber = conf.getBoolean(
          HDDS_CONTAINER_SCRUB_ENABLED,
          HDDS_CONTAINER_SCRUB_ENABLED_DEFAULT);
      if (scrubber) {
        // Data Scrubber needs to be disabled for non-persistent chunks.
        LOG.warn("Failed to set " + HDDS_CONTAINER_PERSISTDATA + " to false."
            + " Please set " + HDDS_CONTAINER_SCRUB_ENABLED
            + " also to false to enable non-persistent containers.");
        persist = true;
      }
    }

    if (!persist) {
      LOG.warn(HDDS_CONTAINER_PERSISTDATA
          + " is set to false. This should be used only for testing."
          + " All user data will be discarded.");
      return new ChunkManagerDummyImpl();
    }

    return new ChunkManagerDispatcher(sync, manager);
  }
}