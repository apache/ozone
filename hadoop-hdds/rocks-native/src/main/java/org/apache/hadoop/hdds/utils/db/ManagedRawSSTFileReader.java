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

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

import java.io.Closeable;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.hadoop.hdds.utils.NativeLibraryLoader;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI for RocksDB RawSSTFileReader.
 */
public class ManagedRawSSTFileReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedRawSSTFileReader.class);

  private final String fileName;
  // Native address of pointer to the object.
  private final long nativeHandle;

  public static boolean tryLoadLibrary() {
    try {
      loadLibrary();
      return true;
    } catch (NativeLibraryNotLoadedException ignored) {
      return false;
    }
  }

  public static boolean loadLibrary() throws NativeLibraryNotLoadedException {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
    if (!NativeLibraryLoader.getInstance().loadLibrary(ROCKS_TOOLS_NATIVE_LIBRARY_NAME, Arrays.asList(
        ManagedRocksObjectUtils.getRocksDBLibFileName()))) {
      throw new NativeLibraryNotLoadedException(ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
    }
    return true;
  }

  public ManagedRawSSTFileReader(final ManagedOptions options, final String fileName, final int readAheadSize) {
    this.fileName = fileName;
    this.nativeHandle = this.newRawSSTFileReader(options.getNativeHandle(), fileName, readAheadSize);
  }

  public <T> ManagedRawSSTFileIterator<T> newIterator(
      Function<ManagedRawSSTFileIterator.KeyValue, T> transformerFunction,
      ManagedSlice fromSlice, ManagedSlice toSlice, IteratorType type) {
    long fromNativeHandle = fromSlice == null ? 0 : fromSlice.getNativeHandle();
    long toNativeHandle = toSlice == null ? 0 : toSlice.getNativeHandle();
    LOG.info("Iterating SST file: {} with native lib. " +
            "LowerBound: {}, UpperBound: {}, type : {} with reader handle: {}", fileName, fromSlice, toSlice, type,
        this.nativeHandle);
    return new ManagedRawSSTFileIterator<>(fileName + " " + this.nativeHandle,
        newIterator(this.nativeHandle, fromSlice != null,
            fromNativeHandle, toSlice != null, toNativeHandle),
        transformerFunction, type);
  }

  private native long newRawSSTFileReader(long optionsHandle, String filePath, int readSize);

  private native long newIterator(long handle, boolean hasFrom, long fromSliceHandle, boolean hasTo,
                                  long toSliceHandle);

  private native void disposeInternal(long handle);

  @Override
  public void close() {
    disposeInternal(nativeHandle);
  }
}
