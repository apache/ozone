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

import java.net.URI;
import java.util.concurrent.Callable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.codahale.metrics.Timer;
import org.apache.hadoop.ozone.util.PayloadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test hsync/write synchronization performance.
 *
 * Example usage:
 *
 * To generate 1000 hsync calls with 10 threads on a single file:
 *    ozone freon hsync-generator -t 10 --bytes-per-write=1024 -n 1000
 *
 * To generate 1000 hsync calls with 10 threads on 3 files simultaneously:
 *
 *    ozone freon hsync-generator -t 10 --bytes-per-write=1024 --number-of-files=3 -n 1000
 *
 */
@Command(name = "hg",
    aliases = "hsync-generator",
    description = "Generate writes and hsync traffic on one or multiple files.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class HsyncGenerator extends BaseFreonGenerator implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(HsyncGenerator.class);

  @CommandLine.ParentCommand
  private Freon freon;

  @Option(names = {"--path"},
      description = "Hadoop FS file system path. Use full path.",
      defaultValue = "o3fs://bucket1.vol1")
  private String rootPath;

  @Option(names = {"--bytes-per-write"},
      description = "Size of each write",
      defaultValue = "1024")
  private int writeSize;

  @Option(names = {"--number-of-files"},
      description = "Number of files to run test.",
      defaultValue = "1")
  private int numberOfFiles;

  private Timer timer;

  private OzoneConfiguration configuration;
  private FileSystem fileSystem;
  private FSDataOutputStream[] outputStreams;
  private byte[] data;

  public HsyncGenerator() {
  }

  @VisibleForTesting
  HsyncGenerator(OzoneConfiguration ozoneConfiguration) {
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

    outputStreams = new FSDataOutputStream[numberOfFiles];
    for (int i = 0; i < numberOfFiles; i++) {
      Path file = new Path(rootPath + "/" + generateObjectName(i));
      fileSystem.mkdirs(file.getParent());
      outputStreams[i] = fileSystem.create(file);
      LOG.info("Created file for testing: {}", file);
    }

    timer = getMetrics().timer("hsync-generator");
    data = PayloadUtils.generatePayload(writeSize);

    try {
      runTests(this::sendHsync);
    } finally {
      for (FSDataOutputStream outputStream : outputStreams) {
        outputStream.close();
      }
      fileSystem.close();
    }

    return null;
  }

  private void sendHsync(long counter) throws Exception {
    timer.time(() -> {
      FSDataOutputStream outputStream = outputStreams[((int) counter) % numberOfFiles];
      outputStream.write(data);
      outputStream.hsync();
      return null;
    });
  }
}
