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
import java.util.concurrent.Callable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "dfsg",
    aliases = "dfs-file-generator",
    description = "Create random files to the any dfs compatible file system.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class HadoopFsGenerator extends HadoopBaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-s", "--size"},
      description = "Size of the generated files. " +
          StorageSizeConverter.STORAGE_SIZE_DESCRIPTION,
      defaultValue = "10KB",
      converter = StorageSizeConverter.class)
  private StorageSize fileSize;

  @Option(names = {"--buffer"},
      description = "Size of buffer used store the generated key content",
      defaultValue = "10240")
  private int bufferSize;

  @Option(names = {"--copy-buffer"},
      description = "Size of bytes written to the output in one operation",
      defaultValue = "4096")
  private int copyBufferSize;

  @Option(names = {"--sync"},
      description = "Type of operation to execute after a write. Supported " +
      "options include NONE (default), HFLUSH and HSYNC",
      defaultValue = "NONE")
  private static ContentGenerator.SyncOptions flushOrSync;

  private ContentGenerator contentGenerator;

  private Timer timer;

  @Override
  public Void call() throws Exception {
    super.init();

    Path file = new Path(getRootPath() + "/" + generateObjectName(0));
    getFileSystem().mkdirs(file.getParent());

    contentGenerator =
        new ContentGenerator(fileSize.toBytes(), bufferSize, copyBufferSize,
            flushOrSync);

    timer = getMetrics().timer("file-create");

    runTests(this::createFile);

    return null;
  }

  private void createFile(long counter) throws Exception {
    Path file = new Path(getRootPath() + "/" + generateObjectName(counter));
    FileSystem fileSystem = getFileSystem();

    timer.time(() -> {
      try (FSDataOutputStream output = fileSystem.create(file)) {
        contentGenerator.write(output);
      }
      return null;
    });
  }
}
