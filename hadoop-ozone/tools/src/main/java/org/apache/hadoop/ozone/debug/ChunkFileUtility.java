/**
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

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import java.io.File;

/**
 * Utility class to get chunk file path.
 */
public final class ChunkFileUtility {

  /**
   * Private constructor, class is not meant to be initialized.
   */
  private ChunkFileUtility(){}

  public static String getChunkLocationPath(String containerLocation) {
    return containerLocation + File.separator + OzoneConsts.STORAGE_DIR_CHUNKS;
  }

   /**
    * Returns Chunk File Path.
    */
  public static String getChunkFilePath(ContainerProtos.ChunkInfo
        chunkInfo, OmKeyLocationInfo keyLocation,
        ContainerProtos.ContainerDataProto data,
        ChunkLayOutVersion layOutVersion)
        throws StorageContainerException {
    switch (layOutVersion) {
    case FILE_PER_CHUNK:
      return  getChunkLocationPath(data
                  .getContainerPath())
                  + File.separator
                  + chunkInfo.getChunkName();
    case FILE_PER_BLOCK:
      return  getChunkLocationPath(data
                  .getContainerPath())
                  + File.separator
                  + keyLocation.getLocalID() + ".block";
    default:
      throw new StorageContainerException("chunk strategy does not exist",
                  ContainerProtos.Result.UNABLE_TO_FIND_CHUNK);
    }
  }
}
