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

package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for all per-disk DB handles under schema v3.
 */
public final class DatanodeStoreCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeStoreCache.class);

  /**
   * Use container db absolute path as key.
   */
  private final Map<String, RawDB> datanodeStoreMap;
  private static final StoreFactory DEFAULT_STORE_FACTORY =
      (conf, containerDBPath, readOnly) -> new DatanodeStoreSchemaThreeImpl(conf, containerDBPath, readOnly);
  private volatile StoreFactory storeFactory = DEFAULT_STORE_FACTORY;

  private static DatanodeStoreCache cache;
  private boolean miniClusterMode;

  /**
   * Factory to create per-volume schema v3 stores (done this way mainly to ease testing).
   */
  @FunctionalInterface
  public interface StoreFactory {
    DatanodeStore create(ConfigurationSource conf, String containerDBPath, boolean readOnly) throws IOException;
  }

  private DatanodeStoreCache() {
    datanodeStoreMap = new ConcurrentHashMap<>();
  }

  public static synchronized DatanodeStoreCache getInstance() {
    if (cache == null) {
      cache = new DatanodeStoreCache();
    }
    return cache;
  }

  @VisibleForTesting
  public static synchronized void setMiniClusterMode() {
    getInstance().miniClusterMode = true;
  }

  @VisibleForTesting
  public static synchronized void setMiniClusterMode(boolean isMiniCluster) {
    getInstance().miniClusterMode = isMiniCluster;
  }

  @VisibleForTesting
  public synchronized void setStoreFactory(StoreFactory newStoreFactory) {
    storeFactory = Objects.requireNonNull(newStoreFactory, "newStoreFactory == null");
  }

  @VisibleForTesting
  public synchronized void resetStoreFactory() {
    storeFactory = DEFAULT_STORE_FACTORY;
  }

  public void addDB(String containerDBPath, RawDB db) {
    datanodeStoreMap.putIfAbsent(containerDBPath, db);
    LOG.info("Added db {} to cache", containerDBPath);
  }

  private RawDB createRawDB(String containerDBPath, ConfigurationSource conf,
      boolean readOnly) throws IOException {
    DatanodeStore store = storeFactory.create(conf, containerDBPath, readOnly);
    return new RawDB(store, containerDBPath);
  }

  private RawDB createRawDBWithReadOnlyFallback(String containerDBPath,
      ConfigurationSource conf) throws IOException {
    try {
      RawDB readWriteDb = createRawDB(containerDBPath, conf, false);
      LOG.info("Opened db {} in read-write mode", containerDBPath);
      return readWriteDb;
    } catch (IOException readWriteOpenException) {
      if (!RocksDatabaseException.isNoSpaceError(readWriteOpenException)) {
        throw readWriteOpenException;
      }
      LOG.warn("Failed to open db {} in read-write mode due to no space. "
              + "Retrying read-only.", containerDBPath, readWriteOpenException);
      try {
        RawDB readOnlyDb = createRawDB(containerDBPath, conf, true);
        LOG.warn("Opened db {} in read-only mode after read-write open "
            + "failed due to no space", containerDBPath);
        return readOnlyDb;
      } catch (IOException readOnlyOpenException) {
        LOG.error("Failed to open db {} in read-only mode after read-write "
            + "open failed due to no space", containerDBPath,
            readOnlyOpenException);
        readOnlyOpenException.addSuppressed(readWriteOpenException);
        throw readOnlyOpenException;
      }
    }
  }

  public RawDB getDB(String containerDBPath, ConfigurationSource conf)
      throws IOException {
    RawDB db = datanodeStoreMap.get(containerDBPath);
    if (db == null || db.getStore().isClosed()) {
      synchronized (this) {
        db = datanodeStoreMap.get(containerDBPath);
        if (db != null && db.getStore().isClosed()) {
          datanodeStoreMap.remove(containerDBPath, db);
          db.getStore().stop();
          db = null;
          LOG.info("Removed closed db {} from cache", containerDBPath);
        }
        if (db == null) {
          try {
            db = createRawDBWithReadOnlyFallback(containerDBPath, conf);
            datanodeStoreMap.put(containerDBPath, db);
          } catch (IOException e) {
            LOG.error("Failed to get DB store {}", containerDBPath, e);
            throw new IOException("Failed to get DB store " + containerDBPath, e);
          }
        }
      }
    }
    return db;
  }

  public void removeDB(String containerDBPath) {
    RawDB db = datanodeStoreMap.remove(containerDBPath);
    if (db == null) {
      LOG.debug("DB {} already removed", containerDBPath);
      return;
    }

    db.getStore().stop();
    LOG.info("Removed db {} from cache", containerDBPath);
  }

  public void shutdownCache() {
    if (miniClusterMode) {
      if (!datanodeStoreMap.isEmpty()) {
        LOG.info("Skip clearing cache in mini cluster mode. Entries left: {}",
            new TreeSet<>(datanodeStoreMap.keySet()));
      }
      return;
    }

    for (RawDB db : datanodeStoreMap.values()) {
      db.getStore().stop();
    }
    datanodeStoreMap.clear();
  }

  public int size() {
    return datanodeStoreMap.size();
  }
}
