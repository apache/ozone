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
import org.apache.hadoop.ozone.common.ChecksumByteBuffer;
import org.apache.hadoop.ozone.common.ChecksumByteBufferFactory;
import org.apache.hadoop.ozone.common.ChecksumByteBufferImpl;
import org.apache.hadoop.ozone.common.NativeCheckSumCRC32;
import org.apache.hadoop.ozone.common.PureJavaCrc32ByteBuffer;
import org.apache.hadoop.ozone.common.PureJavaCrc32CByteBuffer;
import org.apache.hadoop.util.NativeCRC32Wrapper;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.PureJavaCrc32C;
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

import java.util.zip.CRC32;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Class to benchmark various CRC implementations. This can be executed via
 *
 * ozone genesis -b BenchmarkCRC
 *
 * However there are some points to keep in mind. java.util.zip.CRC32C is not
 * available until Java 9, therefore if the JVM has a lower version than 9, that
 * implementation will not be tested.
 *
 * The hadoop native libraries will only be tested if libhadoop.so is found on
 * the "-Djava.library.path". libhadoop.so is not currently bundled with Ozone,
 * so it needs to be obtained from a Hadoop build and the test needs to be
 * executed on a compatible OS (ie Linux x86):
 *
 * ozone --jvmargs -Djava.library.path=/home/sodonnell/native genesis -b
 *     BenchmarkCRC
 */
public class BenchMarkCRCStreaming {

  private static int dataSize = 64 * 1024 * 1024;

  @State(Scope.Thread)
  public static class BenchmarkState {

    private final ByteBuffer data = ByteBuffer.allocate(dataSize);

    @Param({"512", "1024", "2048", "4096", "32768", "1048576"})
    private int checksumSize;

    @Param({"pureCRC32", "pureCRC32C", "hadoopCRC32C", "hadoopCRC32",
        "zipCRC32", "zipCRC32C", "nativeCRC32", "nativeCRC32C"})
    private String crcImpl;

    private ChecksumByteBuffer checksum;

    public ChecksumByteBuffer checksum() {
      return checksum;
    }

    public String crcImpl() {
      return crcImpl;
    }

    public int checksumSize() {
      return checksumSize;
    }

    @Setup(Level.Trial)
    public void setUp() {
      switch (crcImpl) {
      case "pureCRC32":
        checksum = new PureJavaCrc32ByteBuffer();
        break;
      case "pureCRC32C":
        checksum = new PureJavaCrc32CByteBuffer();
        break;
      case "hadoopCRC32":
        checksum = new ChecksumByteBufferImpl(new PureJavaCrc32());
        break;
      case "hadoopCRC32C":
        checksum = new ChecksumByteBufferImpl(new PureJavaCrc32C());
        break;
      case "zipCRC32":
        checksum = new ChecksumByteBufferImpl(new CRC32());
        break;
      case "zipCRC32C":
        try {
          checksum = new ChecksumByteBufferImpl(
              ChecksumByteBufferFactory.Java9Crc32CFactory.createChecksum());
        } catch (Throwable e) {
          throw new RuntimeException("zipCRC32C is not available pre Java 9");
        }
        break;
      case "nativeCRC32":
        if (NativeCRC32Wrapper.isAvailable()) {
          checksum = new ChecksumByteBufferImpl(new NativeCheckSumCRC32(
              NativeCRC32Wrapper.CHECKSUM_CRC32, checksumSize));
        } else {
          throw new RuntimeException("Native library is not available");
        }
        break;
      case "nativeCRC32C":
        if (NativeCRC32Wrapper.isAvailable()) {
          checksum = new ChecksumByteBufferImpl(new NativeCheckSumCRC32(
              NativeCRC32Wrapper.CHECKSUM_CRC32C, checksumSize));
        } else {
          throw new RuntimeException("Native library is not available");
        }
        break;
      default:
      }
      data.clear();
      data.put(RandomUtils.nextBytes(data.remaining()));
    }
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 2000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void runCRC(Blackhole blackhole, BenchmarkState state) {
    ByteBuffer data = state.data;
    data.clear();
    ChecksumByteBuffer csum = state.checksum;
    int bytesPerCheckSum = state.checksumSize;

    for (int i=0; i<data.capacity(); i += bytesPerCheckSum) {
      data.position(i);
      data.limit(i+bytesPerCheckSum);
      csum.update(data);
      blackhole.consume(csum.getValue());
      csum.reset();
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
