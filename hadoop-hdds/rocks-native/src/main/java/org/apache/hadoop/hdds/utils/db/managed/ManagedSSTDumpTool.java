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

import java.io.FileNotFoundException;
import java.util.Map;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

/**
 * JNI for RocksDB SSTDumpTool.
 */
public class ManagedSSTDumpTool {

  static {
    NativeLibraryLoader.getInstance()
            .loadLibrary(ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
  }

  public ManagedSSTDumpTool() throws NativeLibraryNotLoadedException {
    if (!NativeLibraryLoader.getInstance()
            .isLibraryLoaded(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)) {
      throw new NativeLibraryNotLoadedException(
              ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
    }

  }

  public void run(String[] args) {
    this.runInternal(args);
  }

  public void run(Map<String, String> args) {
    this.run(args.entrySet().stream().map(e -> "--"
            + (e.getValue() == null || e.getValue().isEmpty() ? e.getKey() :
            e.getKey() + "=" + e.getValue())).toArray(String[]::new));
  }

  private native void runInternal(String[] args);

  public static void main(String[] args)
          throws NativeLibraryNotLoadedException, FileNotFoundException {
    new ManagedSSTDumpTool().run(args);
  }
}
