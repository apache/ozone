/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.container.stream.DirectoryServerDestination;
import org.apache.hadoop.ozone.container.stream.DirectoryServerSource;
import org.apache.hadoop.ozone.container.stream.StreamingClient;
import org.apache.hadoop.ozone.container.stream.StreamingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

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
          description = "Size of the generated files.",
          defaultValue = "104857600")
  private int fileSize;


  private int port = 1234;

  private String subdir = "dir1";
  private Timer timer;


  @Override
  public Void call() throws Exception {
    init();

    generateBaseData();

    timer = getMetrics().timer("streaming");
    setThreadNo(1);
    runTests(this::copyDir);

    return null;
  }

  private void generateBaseData() throws IOException {
    Path sourceDir = testRoot.resolve("streaming-0");
    if (Files.exists(sourceDir)) {
      deleteDirRecursive(sourceDir);
    }
    Path subDir = sourceDir.resolve(subdir);
    Files.createDirectories(subDir);
    ContentGenerator contentGenerator = new ContentGenerator(fileSize,
        1024);

    for (int i = 0; i < numberOfFiles; i++) {
      try (FileOutputStream out = new FileOutputStream(
          subDir.resolve("file-" + i).toFile())
      ) {
        contentGenerator.write(out);
      }
    }
  }

  private void copyDir(long l) {
    Path sourceDir = testRoot.resolve("streaming-" + l);
    Path destinationDir = testRoot.resolve("streaming-" + (l + 1));

    try (StreamingServer server =
             new StreamingServer(new DirectoryServerSource(sourceDir),
                 1234)) {
      try {
        server.start();
        LOG.info("Starting streaming server on port {} to publish dir {}",
            port, sourceDir);

        try (StreamingClient client =
                     new StreamingClient("localhost", port,
                             new DirectoryServerDestination(
                                     destinationDir))) {

          timer.time(() -> client.stream(subdir));

        }
        LOG.info("Replication has been finished to {}", sourceDir);

      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }


      deleteDirRecursive(sourceDir);

    }
  }

  private void deleteDirRecursive(Path destinationDir) {
    try {
      FileUtils.forceDelete(destinationDir.toFile());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
