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

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.DirectSlice;

/**
 * Managed {@link DirectSlice} for leak detection.
 * NOTE: This class should be only with ByteBuffer whose position and limit is going be immutable in the lifetime of
 *  this ManagedDirectSlice instance. This means that the ByteBuffer's position and limit should not be modified
 *  externally while the ManagedDirectSlice is in use. The value in the byte buffer should be only accessed via the
 *  instance.
 */
public class ManagedDirectSlice extends DirectSlice {

  private final UncheckedAutoCloseable leakTracker = track(this);

  public ManagedDirectSlice(ByteBuffer buffer) {
    super(Objects.requireNonNull(buffer, "buffer == null").slice());
    setLength(buffer.remaining());
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
}
