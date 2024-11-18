/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBStore;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for interacting with datanode databases.
 */
public interface DBStoreManager extends Closeable {

  /**
   * Start datanode manager.
   *
   * @param configuration - Configuration
   * @throws IOException - Unable to start datanode store.
   */
  void start(ConfigurationSource configuration) throws IOException;

  /**
   * Stop datanode manager.
   */
  void stop() throws Exception;

  /**
   * Get datanode store.
   *
   * @return datanode store.
   */
  DBStore getStore();

  /**
   * Helper to create and write batch transactions.
   */
  BatchOperationHandler getBatchHandler();

  void flushLog(boolean sync) throws IOException;

  void flushDB() throws IOException;

  void compactDB() throws IOException;

  /**
   * Returns if the underlying DB is closed. This call is thread safe.
   * @return true if the DB is closed.
   */
  boolean isClosed();

  default void compactionIfNeeded() throws Exception {
  }
}
