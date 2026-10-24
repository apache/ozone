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

package org.apache.hadoop.hdds.scm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig.MetricsMode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests ozone.client.metrics.mode end to end: a client in its own JVM, as in a CLI command or a YARN container, writes
 * and reads a key on a MiniOzoneCluster. The cluster's services start the metrics system in the test JVM, so the
 * client has to run in a separate one.
 */
@Timeout(300)
public class TestClientMetricsMode {

  private static final String VOLUME = "vol-metrics-mode";
  private static final String BUCKET = "bucket";
  private static final String RESULT = "CLIENT_METRICS_RESULT";
  private static final Pattern RESULT_LINE = Pattern.compile(
      RESULT + " started=(\\w+) sourceMBean=(\\w+) registered=(\\w+) ops=(\\d+)");
  private static final String SINK_FILE = "client-metrics.out";

  private static MiniOzoneCluster cluster;

  @TempDir
  private Path dir;

  /** A client configuration, and whether the client's metrics system is expected to start. */
  enum Case {
    DEFAULT_WITHOUT_SINK(null, false, false),
    DEFAULT_WITH_SINK(null, true, true),
    ALWAYS_WITHOUT_SINK(MetricsMode.ALWAYS, false, true),
    NEVER_WITH_SINK(MetricsMode.NEVER, true, false);

    private final MetricsMode mode;
    private final boolean sink;
    private final boolean starts;

    Case(MetricsMode mode, boolean sink, boolean starts) {
      this.mode = mode;
      this.sink = sink;
      this.starts = starts;
    }
  }

  @BeforeAll
  public static void init() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration()).build();
    cluster.waitForClusterToBeReady();
    try (OzoneClient client = cluster.newClient()) {
      client.getObjectStore().createVolume(VOLUME);
      client.getObjectStore().getVolume(VOLUME).createBucket(BUCKET);
    }
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @EnumSource(Case.class)
  void clientStartsMetricsSystemOnlyWhenExpected(Case c) throws Exception {
    // the child's configuration directory, ahead of the test resources on its classpath
    Path confDir = Files.createDirectories(dir.resolve("conf"));
    OzoneConfiguration conf = new OzoneConfiguration(cluster.getConf());
    if (c.mode != null) {
      conf.set("ozone.client.metrics.mode", c.mode.name());
    }
    try (OutputStream out = Files.newOutputStream(confDir.resolve("ozone-site.xml"))) {
      conf.writeXml(out);
    }
    Path sinkFile = dir.resolve(SINK_FILE);
    if (c.sink) {
      Files.write(confDir.resolve("hadoop-metrics2.properties"), (
          "*.sink.file.class=org.apache.hadoop.metrics2.sink.FileSink\n"
          + "xceiverclientmetrics.sink.file.filename=" + sinkFile + "\n").getBytes(UTF_8));
    }

    String log = runClient(confDir, c.name());
    Matcher result = RESULT_LINE.matcher(log);
    assertTrue(result.find(), "No result from the client:\n" + log);
    assertEquals(c.starts, Boolean.parseBoolean(result.group(1)), "metrics system started\n" + log);
    assertEquals(c.starts, Boolean.parseBoolean(result.group(2)), "client metrics MBean registered\n" + log);
    // in every mode the client metrics are registered and counted, so a metrics system started later publishes them
    assertTrue(Boolean.parseBoolean(result.group(3)), "client metrics registered\n" + log);
    assertTrue(Long.parseLong(result.group(4)) > 0, "client ops counted\n" + log);
    if (c.sink) {
      // FileSink opens its file when the metrics system starts
      assertEquals(c.starts, Files.exists(sinkFile), "sink file " + sinkFile);
    }
  }

  private String runClient(Path confDir, String key) throws Exception {
    Path output = dir.resolve("client.log");
    String classpath = confDir + File.pathSeparator
        + System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process process = new ProcessBuilder(
        Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", classpath, TestClientMetricsMode.class.getName(), key)
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      boolean exited = process.waitFor(120, TimeUnit.SECONDS);
      String log = new String(Files.readAllBytes(output), UTF_8);
      assertTrue(exited, "Client did not finish:\n" + log);
      assertEquals(0, process.exitValue(), log);
      return log;
    } finally {
      process.destroyForcibly();
    }
  }

  /** The client process: writes and reads a key, then reports the state of its metrics. */
  public static void main(String[] args) throws Exception {
    byte[] data = ("metrics mode " + args[0]).getBytes(UTF_8);
    try (OzoneClient client = OzoneClientFactory.getRpcClient(new OzoneConfiguration())) {
      OzoneBucket bucket = client.getObjectStore().getVolume(VOLUME).getBucket(BUCKET);
      try (OzoneOutputStream out = bucket.createKey(args[0], data.length)) {
        out.write(data);
      }
      try (OzoneInputStream in = bucket.readKey(args[0])) {
        assertArrayEquals(data, IOUtils.toByteArray(in));
      }
      // before closing the client, which unregisters its metrics
      MBeanServer mbeans = ManagementFactory.getPlatformMBeanServer();
      String service = "Hadoop:service=" + XceiverClientMetrics.SOURCE_NAME;
      System.out.println(RESULT
          + " started=" + mbeans.isRegistered(new ObjectName(service + ",name=MetricsSystem,sub=Control"))
          + " sourceMBean=" + mbeans.isRegistered(new ObjectName(service + ",name=" + XceiverClientMetrics.SOURCE_NAME))
          + " registered=" + (DefaultMetricsSystem.instance().getSource(XceiverClientMetrics.SOURCE_NAME) != null)
          + " ops=" + XceiverClientManager.getXceiverClientMetrics().getTotalOpCount());
    }
  }
}
