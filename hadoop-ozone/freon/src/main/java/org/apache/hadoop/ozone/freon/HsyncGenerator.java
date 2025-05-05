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
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.util.PayloadUtils;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test hsync/write synchronization performance.
 * This tool simulates the way HBase writes transaction logs (WAL) to a file in Ozone:
 *  - Transactions are written to the file's OutputStream by a single thread, each transaction is numbered by an
 *  increasing counter. Every transaction can be serialized to the OutputStream via multiple write calls.
 *  - Multiple threads checks and sync (hsync) the OutputStream to make it persistent.
 *
 * Example usage:
 *
 * To simulate hlog that generates 1M hsync calls with 5 threads:
 *
 *    ozone freon hsync-generator -t 5 --writes-per-transaction=32 --bytes-per-write=8 -n 1000000
 *
 */
@Command(name = "hg",
    aliases = "hsync-generator",
    description = "Generate writes and hsync traffic on one or multiple files.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
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
      defaultValue = "8")
  private int writeSize;

  @Option(names = {"--writes-per-transaction"},
      description = "Size of each write",
      defaultValue = "32")
  private int writesPerTransaction;

  private Timer timer;

  private OzoneConfiguration configuration;
  private FSDataOutputStream outputStream;
  private byte[] data;
  private final BlockingQueue<Integer> writtenTransactions = new ArrayBlockingQueue<>(10_000);
  private final AtomicInteger lastSyncedTransaction = new AtomicInteger();

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
      configuration = freon.getOzoneConf();
    }
    URI uri = URI.create(rootPath);

    FileSystem fileSystem = FileSystem.get(uri, configuration);
    Path file = new Path(rootPath + "/" + generateObjectName(0));
    fileSystem.mkdirs(file.getParent());
    outputStream = fileSystem.create(file);

    LOG.info("Created file for testing: {}", file);

    timer = getMetrics().timer("hsync-generator");
    data = PayloadUtils.generatePayload(writeSize);

    startTransactionWriter();

    try {
      runTests(this::sendHsync);
    } finally {
      outputStream.close();
      fileSystem.close();
    }

    return null;
  }

  private void startTransactionWriter() {
    Thread transactionWriter = new Thread(this::generateTransactions);
    transactionWriter.setDaemon(true);
    transactionWriter.start();
  }

  private void generateTransactions() {
    int transaction = 0;
    while (true) {
      for (int i = 0; i < writesPerTransaction; i++) {
        try {
          if (writeSize > 1) {
            outputStream.write(data);
          } else {
            outputStream.write(i);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      try {
        writtenTransactions.put(transaction++);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void sendHsync(long counter) throws Exception {
    timer.time(() -> {
      while (true) {
        int transaction = writtenTransactions.take();
        int lastSynced = lastSyncedTransaction.get();
        if (transaction > lastSynced) {
          outputStream.hsync();
          lastSyncedTransaction.compareAndSet(lastSynced, transaction);
          return null;
        }
      }
    });
  }
}
