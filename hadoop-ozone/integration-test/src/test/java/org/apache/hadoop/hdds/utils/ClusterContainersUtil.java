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

package org.apache.hadoop.hdds.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;

/**
 * Utility method to manipulate/inspect container data on disk in a mini cluster.
 */
public final class ClusterContainersUtil {
  private ClusterContainersUtil() {
  }

  /**
   *
   *
   * @param cluster a mini ozone cluster object.
   * @param container a container object.
   * @param key an OzoneKey object.
   * @return the location of the chunk file.
   * @throws IOException
   */
  public static File getChunksLocationPath(MiniOzoneCluster cluster, Container container, OzoneKey key)
      throws IOException {
    Preconditions.checkArgument(key instanceof OzoneKeyDetails);
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();
    long localID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getLocalID();
    // From the containerData, get the block iterator for all the blocks in
    // the container.
    KeyValueContainerData containerData =
        (KeyValueContainerData) container.getContainerData();
    try (DBHandle db = BlockUtils.getDB(containerData, cluster.getConf())) {
      BlockID blockID = new BlockID(containerID, localID);
      String blockKey = containerData.getBlockKey(localID);
      BlockData blockData = db.getStore().getBlockByID(blockID, blockKey);
      assertNotNull(blockData, "Block not found");

      // Get the location of the chunk file
      String containreBaseDir =
          container.getContainerData().getVolume().getHddsRootDir().getPath();
      File chunksLocationPath = KeyValueContainerLocationUtil
          .getChunksLocationPath(containreBaseDir, cluster.getClusterId(), containerID);
      return chunksLocationPath;
    }
  }

  /**
   * Corrupt the chunk backing the key in a mini cluster.
   * @param cluster a mini ozone cluster object.
   * @param container a container object.
   * @param key an OzoneKey object.
   * @throws IOException
   */
  public static void corruptData(MiniOzoneCluster cluster, Container container, OzoneKey key)
      throws IOException {
    File chunksLocationPath = getChunksLocationPath(cluster, container, key);
    byte[] corruptData = "corrupted data".getBytes(UTF_8);
    // Corrupt the contents of chunk files
    for (File file : FileUtils.listFiles(chunksLocationPath, null, false)) {
      FileUtils.writeByteArrayToFile(file, corruptData);
    }
  }

  /**
   * Inspect and verify if chunk backing the key in a mini cluster is the same as the string.
   * @param cluster a mini ozone cluster object.
   * @param container a container object.
   * @param key an OzoneKey object.
   * @return true if the same; false if does not match.
   * @throws IOException
   */
  public static boolean verifyOnDiskData(MiniOzoneCluster cluster, Container container, OzoneKey key, String data)
      throws IOException {
    File chunksLocationPath = getChunksLocationPath(cluster, container, key);
    for (File file : FileUtils.listFiles(chunksLocationPath, null, false)) {
      String chunkOnDisk = FileUtils.readFileToString(file, Charset.defaultCharset());
      if (!data.equals(chunkOnDisk)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return the first container object in a mini cluster specified by its ID.
   * @param cluster a mini ozone cluster object.
   * @param containerID a long variable representing cluater ID.
   * @return the container object; null if not found.
   */
  public static Container getContainerByID(MiniOzoneCluster cluster, long containerID) {
    // Get the container by traversing the datanodes. Atleast one of the
    // datanode must have this container.
    Container container = null;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        break;
      }
    }
    return container;
  }
}
