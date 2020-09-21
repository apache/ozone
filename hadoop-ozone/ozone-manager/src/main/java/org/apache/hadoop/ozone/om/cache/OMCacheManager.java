/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.cache;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;

import java.io.IOException;

/**
 * Manages cache to do a faster path look ups. OM will traverse each path
 * components in the path from parent to the child leaf node.
 */
public class OMCacheManager {

  private final CacheStore dirCache;

  public OMCacheManager(OzoneConfiguration config) throws IOException {
    // Defaulting to DIR_LRU cache policy.
    dirCache = OMMetadataCacheFactory.getCache(
            OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY,
            OMConfigKeys.OZONE_OM_CACHE_DIR_DEFAULT, config);
  }

  public CacheStore getDirCache() {
    return dirCache;
  }
}
