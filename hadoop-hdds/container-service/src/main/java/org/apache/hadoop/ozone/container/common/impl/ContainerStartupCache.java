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

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.container.common.impl.ContainerData.CHARSET_ENCODING;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container YAML cache for reducing random IO when reading Container YAML files.
 */
public class ContainerStartupCache {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerStartupCache.class);
  private static final String CACHE_FILE_NAME = "container_cache.json";
  private static final String CACHE_FILE_TEMP_SUFFIX = ".tmp";
  private static final String CHECKSUM_PLACEHOLDER = "CHECKSUM_PLACEHOLDER";

  private final ObjectMapper objectMapper;

  /**
   * Versioning for Container Cache format.
   */
  public enum CacheVersion {
    INITIAL_VERSION(1, "Initial container cache format"),

    FUTURE_VERSION(-1, "Used internally when an unknown cache version is encountered");

    public static final CacheVersion CURRENT = latest();
    public static final int CURRENT_VERSION = CURRENT.version;

    private final int version;
    private final String description;

    CacheVersion(int version, String description) {
      this.version = version;
      this.description = description;
    }

    public String description() {
      return description;
    }

    public static CacheVersion fromValue(int value) {
      for (CacheVersion v : values()) {
        if (v.version == value) {
          return v;
        }
      }
      return FUTURE_VERSION;
    }

    private static CacheVersion latest() {
      CacheVersion[] versions = CacheVersion.values();
      // Return the latest version (excluding FUTURE_VERSION which is always last)
      return versions[versions.length - 2];
    }
  }

  public ContainerStartupCache() {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  /**
   * Cache metadata for containers in the volume.
   */
  public static class CacheMetadata {
    private Map<Long, String> containerYamlData;
    private long rocksDbSequenceNumber;
    private String checksum;
    private long timestamp;
    private int version;

    public CacheMetadata() {
      this.containerYamlData = new HashMap<>();
      this.timestamp = System.currentTimeMillis();
      this.version = CacheVersion.CURRENT_VERSION;
    }

    public Map<Long, String> getContainerYamlData() {
      return containerYamlData;
    }

    public void setContainerYamlData(Map<Long, String> containerYamlData) {
      this.containerYamlData = containerYamlData;
    }

    public long getRocksDbSequenceNumber() {
      return rocksDbSequenceNumber;
    }

    public void setRocksDbSequenceNumber(long rocksDbSequenceNumber) {
      this.rocksDbSequenceNumber = rocksDbSequenceNumber;
    }

    public String getChecksum() {
      return checksum;
    }

    public void setChecksum(String checksum) {
      this.checksum = checksum;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }
  }

  /**
   * Dump container metadata cache for specific volume.
   */
  public File dumpContainerCache(Iterable<? extends Container<?>> containers,
      HddsVolume volume, ConfigurationSource config)
      throws IOException {
    Preconditions.checkNotNull(containers);
    Preconditions.checkNotNull(volume);

    File tempCacheFile = null;
    try {
      CacheMetadata cacheMetadata = new CacheMetadata();

      for (Container<?> container : containers) {
        if (container instanceof KeyValueContainer) {
          KeyValueContainerData containerData = ((KeyValueContainer) container).getContainerData();
          // Only dump SCHEMA_V3 Container
          if (!containerData.getSchemaVersion().equals(SCHEMA_V3)) {
            continue;
          }
          if (containerData.getVolume().getStorageID().equals(volume.getStorageID())) {
            long containerId = containerData.getContainerID();
            String yamlData = containerData.getYamlData();
            if (yamlData != null) {
              cacheMetadata.getContainerYamlData().put(containerId, yamlData);
            }
          }
        }
      }

      // Get RocksDB sequence number for validation
      long sequenceNumber = getRocksDbSequenceNumber(volume, config);
      cacheMetadata.setRocksDbSequenceNumber(sequenceNumber);

      // Calculate checksum for integrity check
      String checksum = calculateCacheChecksum(cacheMetadata);
      cacheMetadata.setChecksum(checksum);

      // Write to a temporary file first
      File cacheFile = getCacheFile(volume);
      tempCacheFile = new File(cacheFile.getAbsolutePath() + CACHE_FILE_TEMP_SUFFIX);

      objectMapper.writeValue(tempCacheFile, cacheMetadata);

      // Atomically move to final location
      Files.move(tempCacheFile.toPath(), cacheFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

      LOG.info("Successfully dumped container cache for volume {} with {} containers, " +
              "RocksDB sequence id: {}, cache version: {}, path: {}", volume.getStorageID(),
          cacheMetadata.getContainerYamlData().size(), sequenceNumber,
          CacheVersion.CURRENT_VERSION,
          cacheFile.toPath().toAbsolutePath());

      return cacheFile;
    } catch (IOException e) {
      if (tempCacheFile != null && tempCacheFile.exists()) {
        cleanupTempCacheFile(tempCacheFile);
      }
      LOG.error("Failed to dump container cache for volume {}", volume.getStorageID(), e);
      throw new IOException("Failed to dump container cache", e);
    }
  }

  private void cleanupTempCacheFile(File tempCacheFile) {
    if (tempCacheFile != null && tempCacheFile.exists()) {
      try {
        if (tempCacheFile.delete()) {
          LOG.debug("Cleaned up temporary cache file: {}", tempCacheFile.getAbsolutePath());
        } else {
          LOG.warn("Failed to delete temporary cache file: {}", tempCacheFile.getAbsolutePath());
        }
      } catch (SecurityException e) {
        LOG.warn("Failed to clean up temporary cache file due to security restrictions: {}", 
                 tempCacheFile.getAbsolutePath(), e);
      }
    }
  }

  /**
   * Delete cache file for a volume after container loading is complete.
   *
   * @param volume volume for which to delete the cache file
   */
  public void deleteCacheFile(HddsVolume volume) {
    File cacheFile = getCacheFile(volume);
    if (cacheFile.exists()) {
      try {
        if (cacheFile.delete()) {
          LOG.info("Successfully deleted cache file for volume {} after container loading",
              volume.getStorageID());
        } else {
          LOG.warn("Failed to delete the cache file for volume {}", volume.getStorageID());
        }
      } catch (Exception e) {
        LOG.warn("Exception while deleting the cache file for volume {}",
            volume.getStorageID(), e);
      }
    }
  }

  /**
   * Load container metadata cache for a volume.
   */
  public CacheMetadata loadContainerCache(HddsVolume volume, ConfigurationSource config) {
    File cacheFile = getCacheFile(volume);
    long currentSequenceNumber = getRocksDbSequenceNumber(volume, config);

    if (!cacheFile.exists()) {
      LOG.debug("Cache file does not exist for volume {}", volume.getStorageID());
      return null;
    }

    try {
      CacheMetadata cacheMetadata = objectMapper.readValue(cacheFile, CacheMetadata.class);

      // Validate cache version compatibility
      if (cacheMetadata.getVersion() != CacheVersion.CURRENT_VERSION) {
        LOG.warn("Cache file version mismatch for volume {}, " +
                "cached format version: {}, current format version: {}. Discarding cache.",
            volume.getStorageID(), cacheMetadata.getVersion(), CacheVersion.CURRENT_VERSION);
        return null;
      }

      // Validate checksum integrity
      if (!validateChecksum(cacheMetadata)) {
        LOG.warn("Cache file checksum validation failed for volume {}", volume.getStorageID());
        return null;
      }

      // Validate RocksDB sequence number
      if (cacheMetadata.getRocksDbSequenceNumber() != currentSequenceNumber) {
        LOG.warn("Cache file RocksDB sequence id mismatch for volume {}, " +
                "cached: {}, current: {}", volume.getStorageID(),
            cacheMetadata.getRocksDbSequenceNumber(), currentSequenceNumber);
        return null;
      }

      LOG.info("Successfully loaded container cache for volume {} with {} containers," +
              " RocksDB sequence id {}, cache format version: {}",
          volume.getStorageID(), cacheMetadata.getContainerYamlData().size(),
          cacheMetadata.getRocksDbSequenceNumber(), cacheMetadata.getVersion());

      return cacheMetadata;
    } catch (Exception e) {
      LOG.warn("Failed to load container cache for volume {}, falling back to disk read",
          volume.getStorageID(), e);
      return null;
    }
  }

  private File getCacheFile(HddsVolume volume) {
    return new File(volume.getStorageDir(), CACHE_FILE_NAME);
  }

  private long getRocksDbSequenceNumber(HddsVolume volume, ConfigurationSource config) {
    if (volume.getDbParentDir() == null) {
      return -1;
    }

    File containerDBFile = new File(volume.getDbParentDir(), CONTAINER_DB_NAME);
    if (!containerDBFile.exists()) {
      return -1;
    }

    try {
      RDBStore store = (RDBStore) DatanodeStoreCache.getInstance()
          .getDB(containerDBFile.getAbsolutePath(), config).getStore().getStore();
      return store.getDb().getLatestSequenceNumber();
    } catch (Exception e) {
      LOG.warn("Failed to get RocksDB sequence number for volume {}",
          volume.getStorageID(), e);
      return -1;
    }
  }

  private String calculateCacheChecksum(CacheMetadata cacheMetadata) throws IOException {
    try {
      // Create a copy with checksum set to placeholder
      CacheMetadata tempMetadata = new CacheMetadata();
      tempMetadata.setContainerYamlData(cacheMetadata.getContainerYamlData());
      tempMetadata.setRocksDbSequenceNumber(cacheMetadata.getRocksDbSequenceNumber());
      tempMetadata.setTimestamp(cacheMetadata.getTimestamp());
      tempMetadata.setVersion(cacheMetadata.getVersion());
      tempMetadata.setChecksum(CHECKSUM_PLACEHOLDER);

      String jsonString = objectMapper.writeValueAsString(tempMetadata);
      MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      sha.update(jsonString.getBytes(CHARSET_ENCODING));
      return DigestUtils.sha256Hex(sha.digest());

    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Unable to create Message Digest, " +
          "usually this is a java configuration issue.", e);
    }
  }

  private boolean validateChecksum(CacheMetadata cacheMetadata) throws IOException {
    String originalChecksum = cacheMetadata.getChecksum();
    if (originalChecksum == null) {
      return false;
    }

    String calculatedChecksum = calculateCacheChecksum(cacheMetadata);
    return originalChecksum.equals(calculatedChecksum);
  }
}
