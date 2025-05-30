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

package org.apache.hadoop.hdds.utils.db;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSnapshot;
import org.apache.ratis.util.ReferenceCountedObject;

/**
 * An abstract class that extends the {@code RawSpliterator} and provides functionality for
 * managing snapshots in a reference-counted manner. This class is designed to support
 * parallel and distributed traversal of raw key-value data within a given prefix and start key
 * range by iterating taking a snapshot of the underlying rocksdb.
 */
abstract class RDBRawSpliterator<RAW, KEY, VALUE> extends RawSpliterator<RAW, KEY, VALUE> {

  private final ReferenceCountedObject<ManagedSnapshot> snapshot;

  abstract ManagedSnapshot getNewSnapshot() throws IOException;

  RDBRawSpliterator(KEY keyPrefix, KEY startKey, int maxParallelism, boolean closeOnException,
      List<byte[]> boundaryKeys, ReferenceCountedObject<ManagedSnapshot> snapshot) throws IOException {
    super(keyPrefix, startKey, maxParallelism, closeOnException, boundaryKeys);
    this.snapshot = snapshot;
    this.snapshot.retain();
    initializeIterator();
  }

  RDBRawSpliterator(KEY prefix, KEY startKey, int maxParallelism, boolean closeOnException) throws IOException {
    super(prefix, startKey, maxParallelism, closeOnException);
    ManagedSnapshot rdbSnapshot = getNewSnapshot();
    this.snapshot = ReferenceCountedObject.wrap(rdbSnapshot, () -> { },
        (completelyReleased) -> {
          if (completelyReleased) {
            rdbSnapshot.close();
          }
        });
    this.snapshot.retain();
    initializeIterator();
  }

  @Override
  void closeCallBack() {
    this.snapshot.release();
  }

  ReferenceCountedObject<ManagedSnapshot> getSnapshot() {
    return snapshot;
  }
}
