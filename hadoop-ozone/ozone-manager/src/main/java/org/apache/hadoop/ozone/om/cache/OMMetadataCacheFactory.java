/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.cache;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides different caching policies for cache entities. This can be
 * extended by adding more entities and their caching policies into it.
 * <p>
 * For example, for the directory cache user has to configure following
 * property with cache type. OM will creates specific cache store for the
 * directory based on the configured cache policy.
 * ozone.om.metadata.cache.directory = DIR_LRU
 * <p>
 * One can add new directory policy to OM by defining new cache type say
 * "DIR_LFU" and implements new CacheStore as DirectoryLFUCacheStore.
 * <p>
 * One can add new entity to OM, let's say file to be cached by configuring the
 * property like below and implement specific provider to instantiate the
 * fileCacheStore.
 * ozone.om.metadata.cache.file = FILE_LRU
 */
public final class OMMetadataCacheFactory {
  private static final Logger LOG =
          LoggerFactory.getLogger(OMMetadataCacheFactory.class);

  /**
   * Private constructor, class is not meant to be initialized.
   */
  private OMMetadataCacheFactory() {
  }

  public static CacheStore getCache(String configuredCachePolicy,
                                    String defaultValue,
                                    OzoneConfiguration config) {
    CacheEntity entity = getCacheEntity(configuredCachePolicy);
    if (entity == null) {
      LOG.error("Invalid property configured: {}", configuredCachePolicy);
      throw new IllegalArgumentException("Invalid property configured: "
              + configuredCachePolicy);
    }

    String cachePolicy = config.get(configuredCachePolicy, defaultValue);
    LOG.info("Configured {} with {}", configuredCachePolicy, cachePolicy);
    switch (entity) {
    case DIR:
    default:
      OMMetadataCacheProvider provider = new OMDirectoryCacheProvider(config,
              cachePolicy);
      if (LOG.isDebugEnabled()) {
        LOG.debug("CacheStore initialized with {}:" + provider.getEntity());
      }
      return provider.getCache();
    }
  }

  /**
   * Entityname present at the second last position of configuration name.
   * Here, this function filters out the second last part separated by '.'
   * character and returns corresponding entity to be cached.
   * <p>
   * For example, "ozone.om.metadata.cache.directory.policy", in this
   * property name the 'directory' part represents entity name.
   * In future, one can add new entity by providing new configuration like,
   * "ozone.om.metadata.cache.file.policy" to represents the FILE cache entity.
   *
   * @param configuredCachePolicy property name for the cache policy
   * @return entity to be cached
   */
  private static CacheEntity getCacheEntity(String configuredCachePolicy) {
    String[] parts = StringUtils.split(configuredCachePolicy, '.');
    String entity = parts[parts.length - 2];
    return CacheEntity.getEntity(entity);
  }

  /**
   * Directory Cache provider which will initialise cache store based on the
   * configured cache policy. For any invalid cache policy argument it will
   * return NO_CACHE.
   */
  private static class OMDirectoryCacheProvider
          implements OMMetadataCacheProvider {

    private OzoneConfiguration config;
    private CacheStore dirCache;

    OMDirectoryCacheProvider(OzoneConfiguration configuration,
                             String cacheType) {
      this.config = configuration;
      this.dirCache = getCacheStore(cacheType);
    }

    @Override
    public CacheStore getCache() {
      return dirCache;
    }

    private CacheStore getCacheStore(String cachePolicy) {
      CachePolicy policy = CachePolicy.getPolicy(cachePolicy);
      switch (policy) {
      case DIR_LRU:
        return new DirectoryLRUCacheStore(config);
      case DIR_NOCACHE: // disable cache
      default:
        return new DirectoryNullCacheStore();
      }
    }

    @Override
    public CacheEntity getEntity() {
      return CacheEntity.DIR;
    }
  }
}
