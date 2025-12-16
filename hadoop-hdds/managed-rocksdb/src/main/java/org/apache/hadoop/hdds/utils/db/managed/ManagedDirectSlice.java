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

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB.NOT_FOUND;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.ratis.util.function.CheckedConsumer;
import org.apache.ratis.util.function.CheckedFunction;
import org.rocksdb.DirectSlice;
import org.rocksdb.RocksDBException;

/**
 * ManagedDirectSlice is a managed wrapper around the DirectSlice object. It ensures
 * proper handling of native resources associated with DirectSlice, utilizing
 * the ManagedObject infrastructure to prevent resource leaks. It works in tandem
 * with a ByteBuffer, which acts as the data source for the managed slice.
 *
 * This class overrides certain operations to tightly control the lifecycle and
 * behavior of the DirectSlice it manages. It specifically caters to use cases
 * where the slice is used in RocksDB operations, providing methods for safely
 * interacting with the slice for put-like operations.
 */
public class ManagedDirectSlice extends ManagedObject<DirectSlice> {

  private final ByteBuffer data;

  public ManagedDirectSlice(ByteBuffer data) {
    super(new DirectSlice(data));
    this.data = data;
  }

  @Override
  public DirectSlice get() {
    throw new UnsupportedOperationException("get() is not supported.");
  }

  /**
   * Executes the provided consumer on the internal {@code DirectSlice} after
   * adjusting the slice's prefix and length based on the current position and
   * remaining data in the associated {@code ByteBuffer}. If the consumer throws
   * a {@code RocksDBException}, it is wrapped and rethrown as a
   * {@code RocksDatabaseException}.
   *
   * @param consumer the operation to perform on the managed {@code DirectSlice}.
   *                 The consumer must handle a {@code DirectSlice} and may throw
   *                 a {@code RocksDBException}.
   * @throws RocksDatabaseException if the provided consumer throws a
   *                                {@code RocksDBException}.
   */
  public void putFromBuffer(CheckedConsumer<DirectSlice, ? extends RocksDBException> consumer)
      throws RocksDatabaseException {
    DirectSlice slice = super.get();
    slice.removePrefix(this.data.position());
    slice.setLength(this.data.remaining());
    try {
      consumer.accept(slice);
    } catch (RocksDBException e) {
      throw new RocksDatabaseException("Error while performing put op with directSlice", e);
    }
    data.position(data.limit());
  }

  /**
   * Retrieves data from the associated DirectSlice into the buffer managed by this instance.
   * The supplied function is applied to the DirectSlice to process the data, and the method
   * adjusts the buffer's position and limit based on the result.
   *
   * @param function a function that operates on a DirectSlice and returns the number
   *                 of bytes written to the buffer, or a specific "not found" value
   *                 if the operation fails. The function may throw a RocksDBException.
   * @return the number of bytes written to the buffer if successful, or a specific
   *         "not found" value indicating the requested data was absent.
   * @throws RocksDatabaseException if the provided function throws a RocksDBException,
   *                                wrapping the original exception.
   */
  public int getToBuffer(CheckedFunction<DirectSlice, Integer, ? extends RocksDBException> function)
      throws RocksDatabaseException {
    DirectSlice slice = super.get();
    slice.removePrefix(this.data.position());
    slice.setLength(this.data.remaining());
    try {
      int lengthWritten = function.apply(slice);
      if (lengthWritten != NOT_FOUND) {
        this.data.limit(Math.min(data.limit(), data.position() + lengthWritten));
      }
      return lengthWritten;
    } catch (RocksDBException e) {
      throw new RocksDatabaseException("Error while performing put op with directSlice", e);
    }
  }

  @VisibleForTesting
  DirectSlice getDirectSlice() {
    return super.get();
  }
}
