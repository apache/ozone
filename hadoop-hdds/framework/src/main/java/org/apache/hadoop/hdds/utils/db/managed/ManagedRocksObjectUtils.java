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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.RocksObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to help assert RocksObject closures.
 */
public final class ManagedRocksObjectUtils {
  private ManagedRocksObjectUtils() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(ManagedRocksObjectUtils.class);

  static void assertClosed(RocksObject rocksObject) {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    if (rocksObject.isOwningHandle()) {
      ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
      LOG.warn("{} is not closed properly",
          rocksObject.getClass().getSimpleName());
    }
  }

  static void assertClosed(RocksObject rocksObject, Throwable stack) {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    if (rocksObject.isOwningHandle()) {
      ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
      LOG.warn("{} is not closed properly",
          rocksObject.getClass().getSimpleName(), stack);
    }
  }
}
