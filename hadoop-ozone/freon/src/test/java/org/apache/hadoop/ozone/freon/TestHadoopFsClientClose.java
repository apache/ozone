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

package org.apache.hadoop.ozone.freon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that {@link HadoopFsGenerator} (dfsg) and {@link HadoopFsValidator}
 * (dfsv) close the {@link org.apache.hadoop.fs.FileSystem} instance they open on
 * the main (calling) thread, in addition to the per-worker instances closed by
 * {@code taskLoopCompleted()}. Before HDDS-14474 the main-thread instance leaked.
 */
public class TestHadoopFsClientClose {

  @TempDir
  private Path tempDir;

  private String rootPath;

  @BeforeEach
  void setUp() {
    CountingFileSystem.reset();
    rootPath = "file://" + tempDir.toAbsolutePath();
  }

  @Test
  void generatorClosesEveryFileSystem() {
    int exitCode = runFreon("dfsg",
        "-n", "4",
        "-t", "2",
        "-s", "1KB",
        "--buffer", "1024",
        "--copy-buffer", "1024");

    assertEquals(0, exitCode);
    assertEquals(CountingFileSystem.opened(), CountingFileSystem.closed(),
        "Every FileSystem opened by dfsg must be closed");
  }

  @Test
  void validatorClosesEveryFileSystem() {
    // dfsv reads files written by dfsg, so generate them first under a shared
    // prefix so both commands address the same object names.
    assertEquals(0, runFreon("dfsg",
        "-p", "fsleak",
        "-n", "4",
        "-t", "2",
        "-s", "1KB",
        "--buffer", "1024",
        "--copy-buffer", "1024"));

    CountingFileSystem.reset();

    int exitCode = runFreon("dfsv",
        "-p", "fsleak",
        "-n", "4",
        "-t", "2");

    assertEquals(0, exitCode);
    assertEquals(CountingFileSystem.opened(), CountingFileSystem.closed(),
        "Every FileSystem opened by dfsv must be closed");
  }

  private int runFreon(String command, String... args) {
    String[] prefix = {
        "-D", "fs.file.impl=" + CountingFileSystem.class.getName(),
        command, "-r", rootPath};
    String[] argv = new String[prefix.length + args.length];
    System.arraycopy(prefix, 0, argv, 0, prefix.length);
    System.arraycopy(args, 0, argv, prefix.length, args.length);
    return new Freon().getCmd().execute(argv);
  }

  /**
   * A {@link LocalFileSystem} that counts how many instances are initialized and
   * closed, so a leak of the main-thread instance is observable.
   */
  public static final class CountingFileSystem extends LocalFileSystem {

    private static final AtomicInteger OPENED = new AtomicInteger();
    private static final AtomicInteger CLOSED = new AtomicInteger();

    private boolean counted;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
      super.initialize(name, conf);
      OPENED.incrementAndGet();
    }

    @Override
    public void close() throws IOException {
      if (!counted) {
        counted = true;
        CLOSED.incrementAndGet();
      }
      super.close();
    }

    static void reset() {
      OPENED.set(0);
      CLOSED.set(0);
    }

    static int opened() {
      return OPENED.get();
    }

    static int closed() {
      return CLOSED.get();
    }
  }
}
