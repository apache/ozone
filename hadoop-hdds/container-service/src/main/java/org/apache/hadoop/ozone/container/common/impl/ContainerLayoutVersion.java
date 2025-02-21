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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;

/**
 * Defines layout versions for the Chunks.
 */
public enum ContainerLayoutVersion {

  @Deprecated /* Use FILE_PER_BLOCK instead */
  FILE_PER_CHUNK(1, "One file per chunk") {
    @Override
    public File getChunkFile(File chunkDir, BlockID blockID, String chunkName) {
      return new File(chunkDir, chunkName);
    }
  },
  FILE_PER_BLOCK(2, "One file per block") {
    @Override
    public File getChunkFile(File chunkDir, BlockID blockID, String chunkName) {
      return new File(chunkDir, blockID.getLocalID() + ".block");
    }
  };

  public static final ContainerLayoutVersion
      DEFAULT_LAYOUT = ContainerLayoutVersion.FILE_PER_BLOCK;

  private static final List<ContainerLayoutVersion> CONTAINER_LAYOUT_VERSIONS =
      ImmutableList.copyOf(values());

  private final int version;
  private final String description;

  ContainerLayoutVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  /**
   * Return ContainerLayoutVersion object for the numeric containerVersion.
   */
  public static ContainerLayoutVersion getContainerLayoutVersion(
      int containerVersion) {
    for (ContainerLayoutVersion containerLayoutVersion :
        CONTAINER_LAYOUT_VERSIONS) {
      if (containerLayoutVersion.getVersion() == containerVersion) {
        return containerLayoutVersion;
      }
    }
    return null;
  }

  /**
   * @return list of all versions.
   */
  public static List<ContainerLayoutVersion> getAllVersions() {
    return CONTAINER_LAYOUT_VERSIONS;
  }

  /**
   * @return the latest version.
   */
  public static ContainerLayoutVersion getConfiguredVersion(
      ConfigurationSource conf) {
    try {
      return conf.getEnum(ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY,
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
      BlockID blockID, String chunkName);

  public File getChunkFile(ContainerData containerData, BlockID blockID,
      String chunkName) throws StorageContainerException {
    File chunkDir = ContainerUtils.getChunkDir(containerData);
    return getChunkFile(chunkDir, blockID, chunkName);
  }

  @Override
  public String toString() {
    return "ContainerLayout:v" + version;
  }

}
