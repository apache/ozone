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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.rocksdb.DBOptions;

/**
 * Tests for {@link ManagedDBOptions}, in particular that a logger it is given
 * is closed on replacement and on close, regardless of the reference type used
 * to call setLogger.
 */
public class TestManagedDBOptions {
  static {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  @Test
  public void testSetLoggerViaDBOptionsReferenceIsClosed() {
    ManagedDBOptions managed = new ManagedDBOptions();
    ManagedLogger first = new ManagedLogger(managed, (level, message) -> { });
    ManagedLogger second = new ManagedLogger(managed, (level, message) -> { });

    // Since RocksDB 9.x, DBOptions#setLogger takes a LoggerInterface. Calling
    // it through the parent type must still route through ManagedDBOptions'
    // override (not a bypassed overload) so the logger is tracked and closed.
    DBOptions options = managed;

    options.setLogger(first);
    assertTrue(first.isOwningHandle());

    // Replacing the logger closes the previous one.
    options.setLogger(second);
    assertFalse(first.isOwningHandle(), "previous logger should be closed on replace");
    assertTrue(second.isOwningHandle());

    // Closing the options closes the current logger.
    managed.close();
    assertFalse(second.isOwningHandle(), "current logger should be closed on close");
  }
}
