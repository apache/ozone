/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.hadoop.hdds.utils.NativeLibraryLoader;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

/**
 * JNI for reading data from pipe.
 */
public class PipeInputStream extends InputStream {

  static {
    NativeLibraryLoader.getInstance()
            .loadLibrary(ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
  }
  private byte[] byteBuffer;
  private long nativeHandle;
  private int numberOfBytesLeftToRead;
  private int index = 0;
  private int capacity;

  private AtomicBoolean cleanup;

  PipeInputStream(int capacity) throws NativeLibraryNotLoadedException {
    if (!NativeLibraryLoader.isLibraryLoaded(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)) {
      throw new NativeLibraryNotLoadedException(
              ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
    }

    this.byteBuffer = new byte[capacity];
    this.numberOfBytesLeftToRead = 0;
    this.capacity = capacity;
    this.nativeHandle = newPipe();
    this.cleanup = new AtomicBoolean(false);
  }

  long getNativeHandle() {
    return nativeHandle;
  }

  @Override
  public int read() {
    if (numberOfBytesLeftToRead < 0) {
      this.close();
      return -1;
    }
    if (numberOfBytesLeftToRead == 0) {
      numberOfBytesLeftToRead = readInternal(byteBuffer, capacity,
              nativeHandle);
      index = 0;
      return read();
    }
    numberOfBytesLeftToRead--;
    int ret = byteBuffer[index] & 0xFF;
    index += 1;
    return ret;
  }

  private native long newPipe();

  private native int readInternal(byte[] buff, int numberOfBytes,
                                  long pipeHandle);

  private native void closeInternal(long pipeHandle);

  @Override
  public void close() {
    if (this.cleanup.compareAndSet(false, true)) {
      closeInternal(this.nativeHandle);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }
}
