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
package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  private static DatanodeStoreCache cache;

  private DatanodeStoreCache() {
    datanodeStoreMap = new ConcurrentHashMap<>();
  }

  public static synchronized DatanodeStoreCache getInstance() {
    if (cache == null) {
      cache = new DatanodeStoreCache();
    }
    return cache;
  }

  public void addDB(String containerDBPath, RawDB db) {
    datanodeStoreMap.putIfAbsent(containerDBPath, db);
    LOG.info("Added db {} to cache", containerDBPath);
  }

  public RawDB getDB(String containerDBPath, ConfigurationSource conf)
      throws IOException {
    RawDB db = datanodeStoreMap.get(containerDBPath);
    if (db == null) {
      synchronized (this) {
        db = datanodeStoreMap.get(containerDBPath);
        if (db == null) {
          try {
            DatanodeStore store = new DatanodeStoreSchemaThreeImpl(
                conf, containerDBPath, false);
            db = new RawDB(store, containerDBPath);
            datanodeStoreMap.put(containerDBPath, db);
          } catch (IOException e) {
            LOG.error("Failed to get DB store {}", containerDBPath, e);
            throw new IOException("Failed to get DB store " +
                containerDBPath, e);
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

    try {
      db.getStore().stop();
    } catch (Exception e) {
      LOG.error("Stop DatanodeStore: {} failed", containerDBPath, e);
    }
    LOG.info("Removed db {} from cache", containerDBPath);
  }

  public void shutdownCache() {
    for (Map.Entry<String, RawDB> entry : datanodeStoreMap.entrySet()) {
      try {
        entry.getValue().getStore().stop();
      } catch (Exception e) {
        LOG.warn("Stop DatanodeStore: {} failed", entry.getKey(), e);
      }
    }
    datanodeStoreMap.clear();
  }

  public int size() {
    return datanodeStoreMap.size();
  }
}
