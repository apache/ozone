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

package org.apache.hadoop.fs.ozone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that a JVM which wrote with Ratis data streaming exits without closing its FileSystem, as a MapReduce task
 * does, once its idle datanode clients are closed (HDDS-17278). The client has to run in its own JVM, as the exit is
 * what is tested.
 */
@Timeout(300)
public class TestDataStreamClientExit {

  private static final String RESULT = "DATASTREAM_CLIENT_RESULT";
  private static final Pattern RESULT_LINE = Pattern.compile(RESULT + " streamThreads=(\\d+)");
  private static final long IDLE_THRESHOLD_SECONDS = 1;
  /** The idle threshold, Netty's 2 s quiet period for shutting down the worker group, and a margin. */
  private static final long EXIT_SECONDS = 30;

  private static MiniOzoneCluster cluster;
  private static OzoneBucket bucket;

  @TempDir
  private Path dir;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
    conf.setBoolean(OZONE_FS_DATASTREAM_ENABLED, true);
    // stream every write
    conf.set(OZONE_FS_DATASTREAM_AUTO_THRESHOLD, "1B");
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    try (OzoneClient client = cluster.newClient()) {
      bucket = DataTestUtil.createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    }
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void clientExitsWithoutClosingFileSystem() throws Exception {
    // the child's configuration directory, ahead of the test resources on its classpath
    Path confDir = Files.createDirectories(dir.resolve("conf"));
    OzoneConfiguration conf = new OzoneConfiguration(cluster.getConf());
    conf.set("scm.container.client.idle.threshold", IDLE_THRESHOLD_SECONDS + "s");
    try (OutputStream out = Files.newOutputStream(confDir.resolve("ozone-site.xml"))) {
      conf.writeXml(out);
    }
    String uri = String.format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());

    Path output = dir.resolve("client.log");
    String classpath = confDir + File.pathSeparator
        + System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process process = new ProcessBuilder(
        Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", classpath, TestDataStreamClientExit.class.getName(), uri)
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      // the write, then the exit
      GenericTestUtils.waitFor(() -> !process.isAlive() || log(output).contains(RESULT), 500, 120_000);
      boolean exited = process.waitFor(EXIT_SECONDS, TimeUnit.SECONDS);
      String log = log(output);
      Matcher result = RESULT_LINE.matcher(log);
      assertTrue(result.find(), "No result from the client:\n" + log);
      // the client wrote through data streaming, so without closing its idle clients it would not exit
      assertTrue(Integer.parseInt(result.group(1)) > 0, "No non-daemon data stream thread\n" + log);
      assertTrue(exited, "The client did not exit within " + EXIT_SECONDS + " s after writing:\n" + log);
      assertEquals(0, process.exitValue(), log);
    } finally {
      process.destroyForcibly();
    }
  }

  private static String log(Path output) {
    try {
      return new String(Files.readAllBytes(output), UTF_8);
    } catch (Exception e) {
      return "";
    }
  }

  /** The client process: writes a file with data streaming and returns without closing the FileSystem. */
  public static void main(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(URI.create(args[0]), new OzoneConfiguration());
    try (FSDataOutputStream out = fs.create(new org.apache.hadoop.fs.Path("/file"))) {
      out.write(new byte[1 << 20]);
    }
    long streamThreads = Thread.getAllStackTraces().keySet().stream()
        .filter(t -> !t.isDaemon() && t.getName().startsWith("NettyClientStreamRpc-workerGroup"))
        .count();
    System.out.println(RESULT + " streamThreads=" + streamThreads);
  }
}
