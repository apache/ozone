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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Random positioned reads of one existing key, with threads sharing each input stream.
 */
@Command(name = "ocpr",
    aliases = "ozone-client-positioned-reader",
    description = "Read random ranges of an existing key with positioned reads. "
        + "Threads share the input streams opened on the key.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OzoneClientPositionedReader extends BaseFreonGenerator implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the volume which contains the key.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the key.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-k", "--key"},
      required = true,
      description = "Name of the key to read.")
  private String keyName;

  @Option(names = "--om-service-id",
      description = "OM Service ID")
  private String omServiceID = null;

  @Option(names = "--streams",
      description = "Number of input streams opened on the key. Thread i reads from stream (i mod streams).",
      defaultValue = "1")
  private int streamCount;

  @Option(names = "--clients",
      description = "Number of Ozone clients. Stream j is opened from client (j mod clients).",
      defaultValue = "1")
  private int clientCount;

  @Option(names = "--read-size",
      description = "Size of each positioned read in bytes.",
      defaultValue = "65536")
  private int readSize;

  @Option(names = "--sequential-readers",
      description = "Number of extra threads for each stream that read the stream sequentially during the test.",
      defaultValue = "0")
  private int sequentialReaderCount;

  @Option(names = "--sequential-read-size",
      description = "Size of each sequential read in bytes.",
      defaultValue = "1048576")
  private int sequentialReadSize;

  private final List<OzoneClient> clients = new ArrayList<>();
  private final List<OzoneInputStream> streams = new ArrayList<>();
  private final ThreadLocal<ByteBuffer> buffers = ThreadLocal.withInitial(() -> ByteBuffer.allocate(readSize));

  private long keyLength;
  private Timer timer;

  private ExecutorService sequentialReaders;
  private final List<Future<Void>> sequentialReaderResults = new ArrayList<>();
  private volatile boolean sequentialReadersStopped;
  private Timer sequentialTimer;
  private Counter sequentialBytes;
  private long sequentialStartNanos;

  @Override
  public Void call() throws Exception {
    if (clientCount < 1 || clientCount > streamCount || streamCount > getThreadNo()) {
      throw new IllegalArgumentException("Expected 1 <= --clients <= --streams <= --threads, but got clients="
          + clientCount + ", streams=" + streamCount + ", threads=" + getThreadNo());
    }
    if (readSize <= 0 || sequentialReadSize <= 0 || sequentialReaderCount < 0) {
      throw new IllegalArgumentException(
          "--read-size and --sequential-read-size must be positive, and --sequential-readers must not be negative");
    }

    init();
    OzoneConfiguration conf = createOzoneConfiguration();
    try {
      for (int i = 0; i < clientCount; i++) {
        clients.add(createOzoneClient(omServiceID, conf));
      }
      keyLength = getBucket(0).getKey(keyName).getDataSize();
      if (keyLength < readSize) {
        throw new IllegalArgumentException("Key " + keyName + " has " + keyLength
            + " bytes, which is less than --read-size " + readSize);
      }
      for (int j = 0; j < streamCount; j++) {
        streams.add(getBucket(j % clientCount).readKey(keyName));
      }

      timer = getMetrics().timer("positioned-read");
      startSequentialReaders();
      try {
        runTests(this::readPositioned);
      } finally {
        stopSequentialReaders();
      }
    } finally {
      IOUtils.closeQuietly(streams);
      IOUtils.closeQuietly(clients);
    }
    return null;
  }

  private OzoneBucket getBucket(int clientIndex) throws IOException {
    return clients.get(clientIndex).getObjectStore().getVolume(volumeName).getBucket(bucketName);
  }

  private void readPositioned(long counter) throws Exception {
    OzoneInputStream stream = streams.get((int) (getThreadSequenceId() % streamCount));
    ByteBuffer buffer = buffers.get();
    buffer.clear();
    long position = ThreadLocalRandom.current().nextLong(keyLength / readSize) * readSize;
    timer.time(() -> {
      stream.readFully(position, buffer);
      return null;
    });
  }

  private void startSequentialReaders() {
    if (sequentialReaderCount == 0) {
      return;
    }
    sequentialTimer = getMetrics().timer("sequential-read");
    sequentialBytes = getMetrics().counter("sequential-read-bytes");
    sequentialReaders = Executors.newFixedThreadPool(streamCount * sequentialReaderCount);
    sequentialStartNanos = System.nanoTime();
    for (OzoneInputStream stream : streams) {
      for (int i = 0; i < sequentialReaderCount; i++) {
        sequentialReaderResults.add(sequentialReaders.submit(() -> readSequentially(stream)));
      }
    }
  }

  private Void readSequentially(OzoneInputStream stream) throws Exception {
    byte[] buffer = new byte[sequentialReadSize];
    while (!sequentialReadersStopped) {
      int n = sequentialTimer.time(() -> stream.read(buffer, 0, buffer.length));
      if (n < 0) {
        stream.seek(0);
      } else {
        sequentialBytes.inc(n);
      }
    }
    return null;
  }

  private void stopSequentialReaders() throws Exception {
    if (sequentialReaders == null) {
      return;
    }
    sequentialReadersStopped = true;
    sequentialReaders.shutdown();
    for (Future<Void> result : sequentialReaderResults) {
      result.get();
    }
    double seconds = (System.nanoTime() - sequentialStartNanos) / 1e9;
    print(String.format("Sequential read throughput (MB/s): %.1f", sequentialBytes.getCount() / seconds / 1e6));
  }
}
