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

import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.RocksIterator;

/**
 * Managed RocksIterator.
 *
 * <p>When constructed with a {@code dbRef} (an acquired reference to the
 * underlying RocksDatabase counter), the iterator holds that reference for its
 * entire lifetime and releases it in {@link #close()}. This guarantees the DB
 * cannot be physically destroyed (via waitAndClose) while the iterator is open,
 * eliminating any TOCTOU race between iterator creation and use.
 */
public class ManagedRocksIterator extends ManagedObject<RocksIterator> {

  private final UncheckedAutoCloseable dbRef;

  public ManagedRocksIterator(RocksIterator original, UncheckedAutoCloseable dbRef) {
    super(original);
    this.dbRef = dbRef;
  }

  public ManagedRocksIterator(RocksIterator original) {
    this(original, null);
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      if (dbRef != null) {
        dbRef.close();
      }
    }
  }

  public static ManagedRocksIterator managed(RocksIterator iterator) {
    return new ManagedRocksIterator(iterator);
  }

  public static ManagedRocksIterator managed(RocksIterator iterator, UncheckedAutoCloseable dbRef) {
    return new ManagedRocksIterator(iterator, dbRef);
  }
}
