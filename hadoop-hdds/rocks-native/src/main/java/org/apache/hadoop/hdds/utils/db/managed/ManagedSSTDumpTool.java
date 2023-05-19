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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

/**
 * JNI for RocksDB SSTDumpTool. Pipes the output to an output stream
 */
public class ManagedSSTDumpTool {

  static {
    NativeLibraryLoader.getInstance()
        .loadLibrary(ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
  }

  private int bufferCapacity;
  private ExecutorService executorService;

  public ManagedSSTDumpTool(ExecutorService executorService,
                            int bufferCapacity)
      throws NativeLibraryNotLoadedException {
    if (!NativeLibraryLoader.isLibraryLoaded(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)) {
      throw new NativeLibraryNotLoadedException(
          ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
    }
    this.bufferCapacity = bufferCapacity;
    this.executorService = executorService;
  }

  public SSTDumpToolTask run(String[] args, ManagedOptions options)
      throws NativeLibraryNotLoadedException {
    PipeInputStream pipeInputStream = new PipeInputStream(bufferCapacity);
    return new SSTDumpToolTask(this.executorService.submit(() ->
        this.runInternal(args, options.getNativeHandle(),
            pipeInputStream.getNativeHandle())), pipeInputStream);
  }

  public SSTDumpToolTask run(Map<String, String> args, ManagedOptions options)
      throws NativeLibraryNotLoadedException {
    return this.run(args.entrySet().stream().map(e -> "--"
        + (e.getValue() == null || e.getValue().isEmpty() ? e.getKey() :
        e.getKey() + "=" + e.getValue())).toArray(String[]::new), options);
  }

  private native int runInternal(String[] args, long optionsHandle,
                                 long pipeHandle);

  /**
   * Class holding piped output of SST Dumptool & future of command.
   */
  static class SSTDumpToolTask {
    private Future<Integer> future;
    private InputStream pipedOutput;

    SSTDumpToolTask(Future<Integer> future, InputStream pipedOutput) {
      this.future = future;
      this.pipedOutput = pipedOutput;
    }

    public Future<Integer> getFuture() {
      return future;
    }

    public InputStream getPipedOutput() {
      return pipedOutput;
    }

    public int exitValue() {
      if (this.future.isDone()) {
        try {
          return future.get();
        } catch (InterruptedException | ExecutionException e) {
          return 1;
        }
      }
      return 0;
    }
  }
}
