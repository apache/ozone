/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.impl;


import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;

import java.util.List;

/**
 * Defines layout versions for the Chunks.
 */
public enum ChunkLayOutVersion {

  FILE_PER_CHUNK(1, "One file per chunk"),
  FILE_PER_BLOCK(2, "One file per block");

  private static final ChunkLayOutVersion
      DEFAULT_LAYOUT = ChunkLayOutVersion.FILE_PER_BLOCK;

  private static final List<ChunkLayOutVersion> CHUNK_LAYOUT_VERSIONS =
      ImmutableList.copyOf(values());

  private final int version;
  private final String description;

  ChunkLayOutVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  /**
   * Return ChunkLayOutVersion object for the numeric chunkVersion.
   */
  public static ChunkLayOutVersion getChunkLayOutVersion(int chunkVersion) {
    for (ChunkLayOutVersion chunkLayOutVersion : CHUNK_LAYOUT_VERSIONS) {
      if (chunkLayOutVersion.getVersion() == chunkVersion) {
        return chunkLayOutVersion;
      }
    }
    return null;
  }

  /**
   * @return list of all versions.
   */
  public static List<ChunkLayOutVersion> getAllVersions() {
    return CHUNK_LAYOUT_VERSIONS;
  }

  /**
   * @return the latest version.
   */
  public static ChunkLayOutVersion getConfiguredVersion(Configuration conf) {
    try {
      return conf.getEnum(ScmConfigKeys.OZONE_SCM_CHUNK_LAYOUT_KEY,
          DEFAULT_LAYOUT);
    } catch (IllegalArgumentException e) {
      return DEFAULT_LAYOUT;
    }
  }

  /**
   * @return version number.
   */
  public int getVersion() {
    return version;
  }

  /**
   * @return description.
   */
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return "ChunkLayout:v" + version;
  }
}
