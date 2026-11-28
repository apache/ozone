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

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.track;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;

/**
 * Managed RocksDB {@link Snapshot}: released exactly once on {@link #close()},
 * which also releases the optional DB reference held for its lifetime.
 */
public final class ManagedSnapshot implements UncheckedAutoCloseable {
  private final RocksDB db;
  private final Snapshot snapshot;
  private final UncheckedAutoCloseable dbRef;
  private final UncheckedAutoCloseable leakTracker;
  private final AtomicBoolean released = new AtomicBoolean();

  private ManagedSnapshot(RocksDB db, UncheckedAutoCloseable dbRef) {
    this.db = db;
    this.snapshot = db.getSnapshot();
    this.dbRef = dbRef;
    this.leakTracker = track(this);
  }

  /** Take a snapshot of {@code db}. */
  public static ManagedSnapshot newSnapshot(ManagedRocksDB db,
      UncheckedAutoCloseable dbRef) {
    return new ManagedSnapshot(db.get(), dbRef);
  }

  Snapshot get() {
    return snapshot;
  }

  public boolean isReleased() {
    return released.get();
  }

  @Override
  public void close() {
    if (released.compareAndSet(false, true)) {
      try {
        db.releaseSnapshot(snapshot);
      } finally {
        IOUtils.closeQuietly(dbRef, leakTracker);
      }
    }
  }
}
