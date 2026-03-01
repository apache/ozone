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

import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.Slice;

/**
 * Managed Slice.
 */
public class ManagedSlice extends Slice {
  private final UncheckedAutoCloseable leakTracker = track(this);

  public ManagedSlice(byte[] data) {
    super(data);
  }

  @Override
  public synchronized long getNativeHandle() {
    return super.getNativeHandle();
  }

  @Override
  protected void disposeInternal() {
    // RocksMutableObject.close is final thus can't be decorated.
    // So, we decorate disposeInternal instead to track closure.
    try {
      super.disposeInternal();
    } finally {
      leakTracker.close();
    }
  }

  @Override
  public boolean isOwningHandle() {
    return super.isOwningHandle();
  }
}
