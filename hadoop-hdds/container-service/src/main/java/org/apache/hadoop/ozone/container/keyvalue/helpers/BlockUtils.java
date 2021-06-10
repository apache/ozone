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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import java.io.IOException;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaOneImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNABLE_TO_READ_METADATA_DB;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import static org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil.onFailure;

/**
 * Utils functions to help block functions.
 */
public final class BlockUtils {

  /** Never constructed. **/
  private BlockUtils() {

  }

  /**
   * Obtain a DB handler for a given container. This handler is not cached and
   * the caller must close it after using it.
   * If another thread attempts to open the same container when it is already
   * opened by this thread, the other thread will get a RocksDB exception.
   * @param containerID The containerID
   * @param containerDBPath The absolute path to the container database folder
   * @param schemaVersion The Container Schema version
   * @param conf Configuration
   * @return Handler to the given container.
   * @throws IOException
   */
  public static DatanodeStore getUncachedDatanodeStore(long containerID,
      String containerDBPath, String schemaVersion,
      ConfigurationSource conf, boolean readOnly) throws IOException {

    DatanodeStore store;
    if (schemaVersion.equals(OzoneConsts.SCHEMA_V1)) {
      store = new DatanodeStoreSchemaOneImpl(conf,
          containerID, containerDBPath, readOnly);
    } else if (schemaVersion.equals(OzoneConsts.SCHEMA_V2)) {
      store = new DatanodeStoreSchemaTwoImpl(conf,
          containerID, containerDBPath, readOnly);
    } else {
      throw new IllegalArgumentException(
          "Unrecognized database schema version: " + schemaVersion);
    }
    return store;
  }

  /**
   * Obtain a DB handler for a given container. This handler is not cached and
   * the caller must close it after using it.
   * If another thread attempts to open the same container when it is already
   * opened by this thread, the other thread will get a RocksDB exception.
   * @param containerData The container data
   * @param conf Configuration
   * @return
   * @throws IOException
   */
  public static DatanodeStore getUncachedDatanodeStore(
      KeyValueContainerData containerData, ConfigurationSource conf,
      boolean readOnly) throws IOException {
    return getUncachedDatanodeStore(containerData.getContainerID(),
        containerData.getDbFile().getAbsolutePath(),
        containerData.getSchemaVersion(), conf, readOnly);
  }

  /**
   * Get a DB handler for a given container.
   * If the handler doesn't exist in cache yet, first create one and
   * add into cache. This function is called with containerManager
   * ReadLock held.
   *
   * @param containerData containerData.
   * @param conf configuration.
   * @return DB handle.
   * @throws StorageContainerException
   */
  public static ReferenceCountedDB getDB(KeyValueContainerData containerData,
                                    ConfigurationSource conf) throws
      StorageContainerException {
    Preconditions.checkNotNull(containerData);
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    Preconditions.checkNotNull(containerData.getDbFile());
    try {
      return cache.getDB(containerData.getContainerID(), containerData
          .getContainerDBType(), containerData.getDbFile().getAbsolutePath(),
              containerData.getSchemaVersion(), conf);
    } catch (IOException ex) {
      onFailure(containerData.getVolume());
      String message = String.format("Error opening DB. Container:%s " +
          "ContainerPath:%s", containerData.getContainerID(), containerData
          .getDbFile().getPath());
      throw new StorageContainerException(message, UNABLE_TO_READ_METADATA_DB);
    }
  }
  /**
   * Remove a DB handler from cache.
   *
   * @param container - Container data.
   * @param conf - Configuration.
   */
  public static void removeDB(KeyValueContainerData container,
      ConfigurationSource conf) {
    Preconditions.checkNotNull(container);
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    cache.removeDB(container.getDbFile().getAbsolutePath());
  }

  /**
   * Shutdown all DB Handles.
   *
   * @param cache - Cache for DB Handles.
   */
  public static void shutdownCache(ContainerCache cache)  {
    cache.shutdownCache();
  }

  /**
   * Add a DB handler into cache.
   *
   * @param db - DB handler.
   * @param containerDBPath - DB path of the container.
   * @param conf configuration.
   */
  public static void addDB(ReferenceCountedDB db, String containerDBPath,
      ConfigurationSource conf) {
    ContainerCache cache = ContainerCache.getInstance(conf);
    Preconditions.checkNotNull(cache);
    cache.addDB(containerDBPath, db);
  }

  /**
   * Parses the {@link BlockData} from a bytes array.
   *
   * @param bytes Block data in bytes.
   * @return Block data.
   * @throws IOException if the bytes array is malformed or invalid.
   */
  public static BlockData getBlockData(byte[] bytes) throws IOException {
    try {
      ContainerProtos.BlockData blockData = ContainerProtos.BlockData.parseFrom(
          bytes);
      return BlockData.getFromProtoBuf(blockData);
    } catch (IOException e) {
      throw new StorageContainerException("Failed to parse block data from " +
          "the bytes array.", NO_SUCH_BLOCK);
    }
  }

  /**
   * Verify if request block BCSID is supported.
   *
   * @param container container object.
   * @param blockID requested block info
   * @throws IOException if cannot support block's blockCommitSequenceId
   */
  public static void verifyBCSId(KeyValueContainer container, BlockID blockID)
      throws IOException {
    long bcsId = blockID.getBlockCommitSequenceId();
    Preconditions.checkNotNull(blockID,
        "BlockID cannot be null");
    Preconditions.checkNotNull(container,
        "Container cannot be null");

    long containerBCSId = container.getBlockCommitSequenceId();
    if (containerBCSId < bcsId) {
      throw new StorageContainerException(
          "Unable to find the block with bcsID " + bcsId + " .Container "
              + container.getContainerData().getContainerID() + " bcsId is "
              + containerBCSId + ".", UNKNOWN_BCSID);
    }
  }
}
