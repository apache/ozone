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

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.ozone.container.stream.DirectoryServerDestination;
import org.apache.hadoop.ozone.container.stream.DirectoryServerSource;
import org.apache.hadoop.ozone.container.stream.StreamingClient;
import org.apache.hadoop.ozone.container.stream.StreamingServer;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Freon test for streaming service.
 */
@CommandLine.Command(name = "strmg",
    aliases = "streaming-generator",
    description =
        "Create directory structure and stream them multiple times.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class StreamingGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(StreamingGenerator.class);

  @CommandLine.Option(names = {"--root-dir"},
      description = "Directory where the working directories are created",
      defaultValue = "/tmp/ozone-streaming")
  private Path testRoot;

  @CommandLine.Option(names = {"--files"},
      description = "Number of the files in the test directory " +
          "to be generated.",
      defaultValue = "50")
  private int numberOfFiles;

  @CommandLine.Option(names = {"--size"},
      description = "Size of the generated files. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "100MB",
      converter = StorageSizeConverter.class)
  private StorageSize fileSize;

  private static final String SUB_DIR_NAME = "dir1";

  private ThreadLocal<Integer> counter = new ThreadLocal<>();

  private Timer timer;

  @Override
  public Void call() throws Exception {
    init();

    timer = getMetrics().timer("streaming");
    runTests(this::copyDir);

    return null;
  }

  private void generateBaseData() {
    try {
      final Path sourceDirParent = threadRootDir();
      Path sourceDir = sourceDirParent.resolve("streaming-0");
      if (Files.exists(sourceDirParent)) {
        deleteDirRecursive(sourceDirParent);
      }
      Path subDir = sourceDir.resolve(SUB_DIR_NAME);
      Files.createDirectories(subDir);
      ContentGenerator contentGenerator =
          new ContentGenerator(fileSize.toBytes(), 1024);

      for (int i = 0; i < numberOfFiles; i++) {
        try (OutputStream out = Files.newOutputStream(subDir.resolve("file-" + i))) {
          contentGenerator.write(out);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void copyDir(long l) {
    Integer index = counter.get();
    if (index == null) {
      generateBaseData();
      index = 0;
    }
    Path sourceDir = threadRootDir().resolve("streaming-" + index);
    Path destinationDir = threadRootDir().resolve("streaming-" + (index + 1));
    counter.set(index + 1);

    int port = (int) (1234 + (l % 64000));
    try (StreamingServer server =
             new StreamingServer(new DirectoryServerSource(sourceDir), port)) {
      server.start();
      LOG.info("Starting streaming server on port {} to publish dir {}",
          port, sourceDir);

      try (StreamingClient client =
               new StreamingClient("localhost", port,
                   new DirectoryServerDestination(
                       destinationDir))) {

        timer.time(() -> client.stream(SUB_DIR_NAME));

      }
      LOG.info("Replication has been finished to {}", sourceDir);

      deleteDirRecursive(sourceDir);

    }
  }

  /**
   * Return the thread specific test root directory.
   */
  private Path threadRootDir() {
    return testRoot.resolve(Thread.currentThread().getName());
  }

  private void deleteDirRecursive(Path destinationDir) {
    try {
      FileUtils.forceDelete(destinationDir.toFile());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
