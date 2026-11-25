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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.LOG;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.track;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.DBOptions;
import org.rocksdb.LoggerInterface;

/**
 * Managed DBOptions.
 */
public class ManagedDBOptions extends DBOptions {

  private final UncheckedAutoCloseable leakTracker = track(this);
  private final AtomicReference<LoggerInterface> loggerRef = new AtomicReference<>();

  // DBOptions#setLogger takes LoggerInterface since RocksDB 9.x. Override that
  // exact signature (not the pre-9.x Logger overload) so every call path,
  // including one made through a DBOptions-typed reference, is leak-tracked.
  @Override
  public DBOptions setLogger(LoggerInterface logger) {
    closeLogger(loggerRef.getAndSet(logger));
    return super.setLogger(logger);
  }

  @Override
  public void close() {
    try {
      closeLogger(loggerRef.getAndSet(null));
      super.close();
    } finally {
      leakTracker.close();
    }
  }

  // RocksDB loggers (org.rocksdb.Logger) own native resources and are
  // AutoCloseable; a bare LoggerInterface may not be, so only close when it is.
  private static void closeLogger(LoggerInterface logger) {
    if (logger instanceof AutoCloseable) {
      IOUtils.close(LOG, (AutoCloseable) logger);
    }
  }
}
