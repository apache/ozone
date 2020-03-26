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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.MetadataStore;
import org.apache.hadoop.hdds.utils.MetadataStoreBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.utils.ReferenceDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import javax.annotation.ParametersAreNonnullByDefault;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNABLE_TO_READ_METADATA_DB;


/**
 * Utils functions to help block functions.
 */
public final class BlockUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BlockUtils.class);

  private static final Object cacheLock = new Object();
  @VisibleForTesting
  static LoadingCache<DbHandlerCacheKey, ReferenceDB> cache;

  /** Never constructed. **/
  private BlockUtils() {

  }

  private static void initCache(Configuration conf) {
    if (Objects.isNull(cache)) {
      cache = CacheBuilder.newBuilder()
          .maximumSize(conf.getInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE,
              OzoneConfigKeys.OZONE_CONTAINER_CACHE_DEFAULT))
          .removalListener((notification) ->
              ((ReferenceDB) notification.getValue()).cleanup())
          .concurrencyLevel(1)
          .build(new CacheLoader<DbHandlerCacheKey, ReferenceDB>() {
            @Override
            public ReferenceDB
                load(@ParametersAreNonnullByDefault DbHandlerCacheKey key)
                throws Exception {
              try {
                MetadataStore metadataStore =
                    MetadataStoreBuilder.newBuilder()
                        .setDbFile(new File(key.dbFile))
                        .setCreateIfMissing(false)
                        .setConf(conf)
                        .setDBType(key.dbType)
                        .build();
                return new ReferenceDB(metadataStore, key.dbFile);
              } catch (Exception e) {
                LOG.error("Error opening DB. Container:{} ContainerPath:{}",
                    key.containerId, key.dbType, e);
                throw e;
              }
            }
          });

    }
  }

  /**
   * Get a DB handler for a given container.
   * If the handler doesn't exist in cache yet, first create one and
   * add into cache.
   *
   * @param containerData containerData.
   * @param conf configuration.
   * @return MetadataStore handle.
   * @throws StorageContainerException if could not open a db handler
   */
  public static ReferenceDB getDB(KeyValueContainerData containerData,
                                               Configuration conf)
      throws StorageContainerException {
    try {
      synchronized (cacheLock) {
        initCache(conf);
        Preconditions.checkNotNull(containerData);
        Preconditions.checkNotNull(cache);
        Preconditions.checkNotNull(containerData.getDbFile());
        Preconditions.checkState(containerData.getContainerID() >= 0,
            "Container ID cannot be negative.");
        return cache.get(new DbHandlerCacheKey(containerData));
      }
    } catch (ExecutionException ex) {
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
  public static void removeDB(KeyValueContainerData container, Configuration
      conf) {
    synchronized (cacheLock) {
      Preconditions.checkNotNull(container);
      initCache(conf);
      Preconditions.checkNotNull(cache);
      cache.invalidate(new DbHandlerCacheKey(container));
    }
  }

  /**
   * Shutdown all DB Handles.
   */
  public static void shutdownCache()  {
    synchronized (cacheLock) {
      if(Objects.nonNull(cache)) {
        cache.asMap().values().forEach(ReferenceDB::cleanup);
        cache.invalidateAll();
      }
    }
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

  @VisibleForTesting
  static class DbHandlerCacheKey {
    private long containerId;
    private String dbFile;
    private String dbType;

    private DbHandlerCacheKey(KeyValueContainerData containerData) {
      this.containerId = containerData.getContainerID();
      this.dbFile = containerData.getDbFile().getAbsolutePath();
      this.dbType = containerData.getContainerDBType();
    }

    @VisibleForTesting
    DbHandlerCacheKey(long containerId, String dbFile, String dbType) {
      this.containerId = containerId;
      this.dbFile = dbFile;
      this.dbType = dbType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DbHandlerCacheKey that = (DbHandlerCacheKey) o;
      return containerId == that.containerId &&
          dbFile.equals(that.dbFile) &&
          dbType.equals(that.dbType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(containerId, dbFile, dbType);
    }
  }
}