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

import static org.apache.hadoop.hdds.scm.XceiverClientMetrics.SOURCE_NAME;
import static org.apache.hadoop.hdds.scm.XceiverClientMetrics.isSinkConfigured;
import static org.apache.hadoop.hdds.scm.XceiverClientMetrics.startsMetricsSystem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.management.ObjectName;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig.MetricsMode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests when {@link XceiverClientMetrics} starts the Hadoop metrics system (ozone.client.metrics.mode).
 */
class TestXceiverClientMetricsMode {

  private static final String DEFAULT_FILE = "hadoop-metrics2.properties";
  private static final String CLIENT_FILE = "hadoop-metrics2-xceiverclientmetrics.properties";
  private static final String FILE_SINK_DEFAULTS =
      "*.sink.file.class=org.apache.hadoop.metrics2.sink.FileSink\n*.period=10\n";
  private static final String CLIENT_FILE_SINK = "xceiverclientmetrics.sink.file.filename=client-metrics.out\n";

  @TempDir
  private Path dir;

  @Test
  void noMetricsConfig() throws IOException {
    assertFalse(sinkConfigured());
  }

  @Test
  void sinkDefaultsOnly() throws IOException {
    write(DEFAULT_FILE, FILE_SINK_DEFAULTS);
    assertFalse(sinkConfigured());
  }

  @Test
  void sinkForClientPrefix() throws IOException {
    write(DEFAULT_FILE, FILE_SINK_DEFAULTS + CLIENT_FILE_SINK);
    assertTrue(sinkConfigured());
  }

  @Test
  void sinkForOtherPrefixOnly() throws IOException {
    write(DEFAULT_FILE, FILE_SINK_DEFAULTS + "namenode.sink.file.filename=namenode-metrics.out\n");
    assertFalse(sinkConfigured());
  }

  @Test
  void clientFileWithoutSinkWinsOverDefaultFile() throws IOException {
    write(DEFAULT_FILE, FILE_SINK_DEFAULTS + CLIENT_FILE_SINK);
    write(CLIENT_FILE, "*.period=10\n");
    assertFalse(sinkConfigured());
  }

  @Test
  void clientFileWithSink() throws IOException {
    write(DEFAULT_FILE, FILE_SINK_DEFAULTS);
    write(CLIENT_FILE, FILE_SINK_DEFAULTS + CLIENT_FILE_SINK);
    assertTrue(sinkConfigured());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void startsMetricsSystemByMode(boolean withSink) throws IOException {
    write(DEFAULT_FILE, FILE_SINK_DEFAULTS + (withSink ? CLIENT_FILE_SINK : ""));
    try (URLClassLoader loader = loader()) {
      assertTrue(startsMetricsSystem(MetricsMode.ALWAYS, loader));
      assertFalse(startsMetricsSystem(MetricsMode.NEVER, loader));
      assertEquals(withSink, startsMetricsSystem(MetricsMode.AUTO, loader));
    }
  }

  @Test
  void createWithoutStartingMetricsSystem() throws Exception {
    XceiverClientMetrics.setMetricsMode(MetricsMode.NEVER);
    XceiverClientMetrics metrics = XceiverClientMetrics.create();
    try {
      metrics.incrPendingContainerOpsMetrics(ContainerProtos.Type.ReadChunk);
      assertEquals(1, metrics.getTotalOpCount());
      assertEquals(1, metrics.getPendingContainerOpCountMetrics(ContainerProtos.Type.ReadChunk));
      // registered, so a metrics system started later publishes it
      assertNotNull(DefaultMetricsSystem.instance().getSource(SOURCE_NAME));
      // but this metrics system was not started
      assertFalse(ManagementFactory.getPlatformMBeanServer().isRegistered(
          new ObjectName("Hadoop:service=" + SOURCE_NAME + ",name=MetricsSystem,sub=Control")));
    } finally {
      metrics.unRegister();
      XceiverClientMetrics.setMetricsMode(MetricsMode.AUTO);
    }
  }

  private void write(String name, String content) throws IOException {
    Files.write(dir.resolve(name), content.getBytes(StandardCharsets.UTF_8));
  }

  private boolean sinkConfigured() throws IOException {
    try (URLClassLoader loader = loader()) {
      return isSinkConfigured(SOURCE_NAME, loader);
    }
  }

  /** Sees only the files in {@link #dir}, not the test classpath. */
  private URLClassLoader loader() throws IOException {
    return new URLClassLoader(new URL[] {dir.toUri().toURL()}, null);
  }
}
