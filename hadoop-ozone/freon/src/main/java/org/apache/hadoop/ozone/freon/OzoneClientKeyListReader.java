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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Read a caller-supplied list of existing keys with a warm ozone client and
 * report aggregate read throughput. Unlike {@code ockv} this reads arbitrary,
 * heterogeneous keys and does not validate their content, so it measures the
 * pure warm-client read path against real data.
 */
@Command(name = "ocklr",
    aliases = "ozone-client-key-list-reader",
    description = "Read a list of existing keys (from --key-file) with a warm "
        + "ozone client and report read throughput.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OzoneClientKeyListReader extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientKeyListReader.class);

  // Matches the default Ozone chunk size used by `ozone sh key get`.
  private static final int READ_BUFFER_BYTES = 4 * 1024 * 1024;
  private static final double NANOS_PER_SECOND = 1_000_000_000.0;
  private static final double BYTES_PER_MB = 1_000_000.0;

  @Option(names = {"-v", "--volume"},
      description = "Name of the volume which contains the keys.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the keys.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"--key-file"},
      required = true,
      description = "Local file listing the keys to read, one key name per "
          + "line. Blank lines and lines starting with '#' are ignored.")
  private String keyFile;

  @Option(names = "--om-service-id",
      description = "OM Service ID")
  private String omServiceID;

  private final AtomicLong bytesRead = new AtomicLong();

  private Timer timer;
  private OzoneBucket bucket;
  private List<String> keys;

  @Override
  public Void call() throws Exception {
    init();

    keys = parseKeyLines(Files.readAllLines(Paths.get(keyFile)));

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    try (OzoneClient rpcClient =
        createOzoneClient(omServiceID, ozoneConfiguration)) {
      bucket = rpcClient.getObjectStore()
          .getVolume(volumeName).getBucket(bucketName);

      timer = getMetrics().timer("key-read");

      long startNanos = System.nanoTime();
      runTests(this::readKey);
      double elapsedSeconds =
          (System.nanoTime() - startNanos) / NANOS_PER_SECOND;

      reportThroughput(elapsedSeconds);
    }
    return null;
  }

  private void readKey(long counter) throws Exception {
    String keyName = keys.get((int) (counter % keys.size()));
    bytesRead.addAndGet(timer.time(() -> drain(keyName)));
  }

  private long drain(String keyName) throws IOException {
    byte[] buffer = new byte[READ_BUFFER_BYTES];
    long total = 0;
    try (InputStream in = bucket.readKey(keyName)) {
      int read;
      while ((read = in.read(buffer)) >= 0) {
        total += read;
      }
    }
    return total;
  }

  private void reportThroughput(double elapsedSeconds) {
    long total = bytesRead.get();
    double throughputMBs = total / BYTES_PER_MB / elapsedSeconds;
    LOG.info("Read {} keys, {} bytes in {}s; aggregate {} MB/s",
        timer.getCount(), total, String.format("%.2f", elapsedSeconds),
        String.format("%.1f", throughputMBs));
  }

  static List<String> parseKeyLines(List<String> lines) {
    List<String> keyNames = lines.stream()
        .map(String::trim)
        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
        .collect(Collectors.toList());
    if (keyNames.isEmpty()) {
      throw new IllegalArgumentException(
          "No keys to read (file empty or only comments/blank lines)");
    }
    return keyNames;
  }

}
