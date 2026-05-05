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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Objects;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

/**
 * Class which provides utility methods for container locations.
 */
public final class KeyValueContainerLocationUtil {

  /* Never constructed. */
  private KeyValueContainerLocationUtil() {

  }

  /**
   * Returns Container Metadata Location.
   * @param hddsVolumeDir base dir of the hdds volume where scm directories
   *                      are stored
   * @param clusterId
   * @param containerId
   * @return containerMetadata Path to container metadata location where
   * .container file will be stored.
   */
  public static File getContainerMetaDataPath(String hddsVolumeDir,
                                              String clusterId,
                                              long containerId) {
    return getContainerMetaDataPath(
        getBaseContainerLocation(hddsVolumeDir, clusterId, containerId));
  }

  /**
   * Returns Container Metadata Location.
   * @param containerBaseDir Base container dir
   * @return containerMetadata Path to container metadata location where
   * .container file will be stored.
   */
  public static File getContainerMetaDataPath(String containerBaseDir) {
    String containerMetaDataPath = containerBaseDir + File.separator +
        OzoneConsts.CONTAINER_META_PATH;
    return new File(containerMetaDataPath);
  }

  /**
   * Returns Container Chunks Location.
   * @param baseDir
   * @param clusterId
   * @param containerId
   * @return chunksPath
   */
  public static File getChunksLocationPath(String baseDir, String clusterId,
                                           long containerId) {
    return getChunksLocationPath(
        getBaseContainerLocation(baseDir, clusterId, containerId));
  }

  /**
   * Returns Container Chunks Location.
   * @param containerBaseDir
   * @return chunksPath
   */
  public static File getChunksLocationPath(String containerBaseDir) {
    String chunksPath = containerBaseDir
            + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
    return new File(chunksPath);
  }

  /**
   * Returns base directory for specified container.
   * @param hddsVolumeDir
   * @param clusterId
   * @param containerId
   * @return base directory for container.
   */
  public static String getBaseContainerLocation(String hddsVolumeDir,
                                                 String clusterId,
                                                 long containerId) {
    Objects.requireNonNull(hddsVolumeDir, "hddsVolumeDir == null");
    Objects.requireNonNull(clusterId, "clusterId == null");
    Preconditions.checkState(containerId >= 0,
        "Container Id cannot be negative.");

    String containerSubDirectory = getContainerSubDirectory(containerId);

    String containerMetaDataPath = hddsVolumeDir  + File.separator + clusterId +
        File.separator + Storage.STORAGE_DIR_CURRENT + File.separator +
        containerSubDirectory + File.separator + containerId;

    return containerMetaDataPath;
  }

  /**
   * Returns subdirectory, where this container needs to be placed.
   * @param containerId
   * @return container sub directory
   */
  private static String getContainerSubDirectory(long containerId) {
    int directory = (int) ((containerId >> 9) & 0xFF);
    return Storage.CONTAINER_DIR + directory;
  }

  /**
   * Return containerDB File.
   */
  public static File getContainerDBFile(KeyValueContainerData containerData) {
    if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      final File dbParentDir = containerData.getVolume().getDbParentDir();
      Objects.requireNonNull(dbParentDir, "dbParentDir == null");
      return new File(dbParentDir, OzoneConsts.CONTAINER_DB_NAME);
    }
    final String metadataPath = containerData.getMetadataPath();
    Objects.requireNonNull(metadataPath, "metadataPath == null");
    return new File(metadataPath, containerData.getContainerID() + OzoneConsts.DN_CONTAINER_DB);
  }

}
