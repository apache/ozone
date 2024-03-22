/**
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

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.conf.StorageSize;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test hsync/write synchronization performance.
 */
@Command(name = "ms",
    aliases = "multi-syncer",
    description = "Create a thread pool of hsync threads and a writer thread",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class MultiSyncer extends BaseFreonGenerator
    implements Callable<Void> {

  @CommandLine.ParentCommand
  private Freon freon;

  @Option(names = {"--path"},
      description = "Hadoop FS file system path. Use full path.",
      defaultValue = "o3fs://bucket1.vol1")
  private String rootPath;

  @Option(names = {"-s", "--size"},
      description = "Size of the generated files. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "10MB",
      converter = StorageSizeConverter.class)
  private StorageSize fileSize;

  @Option(names = {"--bytes-per-write"},
      description = "Size of each write",
      defaultValue = "1024")
  private int writeSize;

  @Option(names = {"--syncer-per-writer"},
      description = "Number of sycer threads associated with each writer",
      defaultValue = "5")
  private int syncerPerWriter;

  private ContentGenerator contentGenerator;

  private Timer timer;

  private OzoneConfiguration configuration;
  private FileSystem fileSystem;

  // empy constructor for picocli
  MultiSyncer() {
  }

  @VisibleForTesting
  MultiSyncer(OzoneConfiguration ozoneConfiguration) {
    this.configuration = ozoneConfiguration;
  }


  @Override
  public Void call() throws Exception {
    init();

    if (configuration == null) {
      configuration = freon.createOzoneConfiguration();
    }
    URI uri = URI.create(rootPath);
    fileSystem = FileSystem.get(uri, configuration);

    Path file = new Path(rootPath + "/" + generateObjectName(0));
    fileSystem.mkdirs(file.getParent());

    timer = getMetrics().timer("file-create");

    runTests(this::createFile);

    return null;
  }

  private void createFile(long counter) throws Exception {
    Path file = new Path(rootPath + "/" + generateObjectName(counter));

    contentGenerator =
        new ContentGenerator(fileSize.toBytes(), writeSize, writeSize);

    ExecutorService executor = Executors.newFixedThreadPool(syncerPerWriter);
    timer.time(() -> {
      try (FSDataOutputStream output = fileSystem.create(file)) {
        if (!StoreImplementationUtils.hasCapability(
            output, StreamCapabilities.HSYNC)) {
          throw new UnsupportedOperationException(
              "Abort. The output stream of file " + file + " does not support hsync");
        }

        AtomicBoolean shutdown = new AtomicBoolean();
        startSyncer(executor, output, shutdown);
        contentGenerator.write(output);

        shutdown.set(true);
        executor.shutdown();
      }
      return null;
    });
  }

  void startSyncer(ExecutorService executor, FSDataOutputStream output, AtomicBoolean shutdown) {


    // Create a Runnable task
    Runnable task = () -> {
      // Continuous task to be executed
      while (!shutdown.get()) {
        try {
          output.hsync();
          Thread.sleep(0, 1000 * 10);
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };

    // Submit the task to the ExecutorService
    for (int n = 0; n < syncerPerWriter; n++) {
      executor.submit(task);
    }
  }
}
