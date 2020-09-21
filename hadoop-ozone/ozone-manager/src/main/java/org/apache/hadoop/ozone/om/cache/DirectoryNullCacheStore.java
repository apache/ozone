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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Not caching directories and used to disable caching.
 */
public class DirectoryNullCacheStore implements CacheStore {

  private static final Logger LOG =
          LoggerFactory.getLogger(DirectoryNullCacheStore.class);

  DirectoryNullCacheStore() {
    LOG.info("Initializing DirectoryNullCacheStore..");
  }

  @Override
  public void put(OMCacheKey key, OMCacheValue value) {
    // Nothing to do
  }

  @Override
  public OMCacheValue get(OMCacheKey key) {
    // Nothing to do
    return null;
  }

  @Override
  public void remove(OMCacheKey key) {
    // Nothing to do;
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public CachePolicy getCachePolicy() {
    return CachePolicy.DIR_NOCACHE;
  }
}
