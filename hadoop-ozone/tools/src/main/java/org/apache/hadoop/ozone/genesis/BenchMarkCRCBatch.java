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
package org.apache.hadoop.ozone.genesis;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.util.NativeCRC32Wrapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Class to benchmark hadoop native CRC implementations in batch node.
 *
 * The hadoop native libraries must be available to run this test. libhadoop.so
 * is not currently bundled with Ozone, so it needs to be obtained from a Hadoop
 * build and the test needs to be executed on a compatible OS (ie Linux x86):
 *
 * ozone --jvmargs -Djava.library.path=/home/sodonnell/native genesis -b
 *     BenchmarkCRCBatch
 */
public class BenchMarkCRCBatch {

  private static int dataSize = 64 * 1024 * 1024;

  /**
   * Benchmark state.
   */
  @State(Scope.Thread)
  public static class BenchmarkState {

    private final ByteBuffer data = ByteBuffer.allocate(dataSize);

    @Param({"512", "1024", "2048", "4096", "32768", "1048576"})
    private int checksumSize;

    @Param({"nativeCRC32", "nativeCRC32C"})
    private String crcImpl;

    private byte[] checksumBuffer;
    private int nativeChecksumType = 1;

    public ByteBuffer data() {
      return data;
    }

    public int checksumSize() {
      return checksumSize;
    }

    public String crcImpl() {
      return crcImpl;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value="EI_EXPOSE_REP",
        justification="The intent is to expose this variable")
    public byte[] checksumBuffer() {
      return checksumBuffer;
    }

    public int nativeChecksumType() {
      return nativeChecksumType;
    }

    @Setup(Level.Trial)
    public void setUp() {
      switch (crcImpl) {
      case "nativeCRC32":
        if (NativeCRC32Wrapper.isAvailable()) {
          nativeChecksumType = NativeCRC32Wrapper.CHECKSUM_CRC32;
          checksumBuffer = new byte[4 * dataSize / checksumSize];
        } else {
          throw new RuntimeException("Native library is not available");
        }
        break;
      case "nativeCRC32C":
        if (NativeCRC32Wrapper.isAvailable()) {
          nativeChecksumType = NativeCRC32Wrapper.CHECKSUM_CRC32C;
          checksumBuffer = new byte[4 * dataSize / checksumSize];
        } else {
          throw new RuntimeException("Native library is not available");
        }
        break;
      default:
      }
      data.put(RandomUtils.nextBytes(data.remaining()));
    }
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 2000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void runCRCNativeBatch(Blackhole blackhole, BenchmarkState state) {
    if (state.crcImpl.equals("nativeCRC32")
        || state.crcImpl.equals("nativeCRC32C")) {
      NativeCRC32Wrapper.calculateChunkedSumsByteArray(
          state.checksumSize, state.nativeChecksumType, state.checksumBuffer,
          0, state.data.array(), 0, state.data.capacity());
      blackhole.consume(state.checksumBuffer);
    } else {
      throw new RuntimeException("Batch mode not available for "
          + state.crcImpl);
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
