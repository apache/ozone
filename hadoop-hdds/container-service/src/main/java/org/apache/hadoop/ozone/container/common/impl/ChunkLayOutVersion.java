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


import java.io.File;
import java.util.List;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_FIND_DATA_DIR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines layout versions for the Chunks.
 */
public enum ChunkLayOutVersion {

  FILE_PER_CHUNK(1, "One file per chunk") {
    @Override
    public File getChunkFile(File chunkDir, BlockID blockID,
        ChunkInfo info) {
      return new File(chunkDir, info.getChunkName());
    }
  },
  FILE_PER_BLOCK(2, "One file per block") {
    @Override
    public File getChunkFile(File chunkDir, BlockID blockID,
        ChunkInfo info) {
      return new File(chunkDir, blockID.getLocalID() + ".block");
    }
  };

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkLayOutVersion.class);

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
  public static ChunkLayOutVersion getConfiguredVersion(
      ConfigurationSource conf) {
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

  public abstract File getChunkFile(File chunkDir,
      BlockID blockID, ChunkInfo info);

  public File getChunkFile(ContainerData containerData, BlockID blockID,
      ChunkInfo info) throws StorageContainerException {
    File chunkDir = getChunkDir(containerData);
    return getChunkFile(chunkDir, blockID, info);
  }

  @Override
  public String toString() {
    return "ChunkLayout:v" + version;
  }

  private static File getChunkDir(ContainerData containerData)
      throws StorageContainerException {
    Preconditions.checkNotNull(containerData, "Container data can't be null");

    String chunksPath = containerData.getChunksPath();
    if (chunksPath == null) {
      LOG.error("Chunks path is null in the container data");
      throw new StorageContainerException("Unable to get Chunks directory.",
          UNABLE_TO_FIND_DATA_DIR);
    }
    return new File(chunksPath);
  }

}
