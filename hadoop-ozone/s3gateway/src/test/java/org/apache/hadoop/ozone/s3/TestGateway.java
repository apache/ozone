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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.hdds.server.http.TestHttpServer2;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Tests S3 Gateway startup behavior.
 */
public class TestGateway {

  private final Logger rootLogger = LogManager.getLogManager().getLogger("");
  private Handler[] originalHandlers;

  @BeforeEach
  void saveRootLoggerHandlers() {
    originalHandlers = rootLogger.getHandlers();
    removeRootLoggerHandlers();
  }

  @AfterEach
  void restoreRootLoggerHandlers() {
    removeRootLoggerHandlers();
    for (Handler handler : originalHandlers) {
      rootLogger.addHandler(handler);
    }
  }

  @Test
  void redirectsJulLogsToSlf4j() {
    Logger julLogger = Logger.getLogger(TestGateway.class.getName());
    LogCapturer logCapturer = LogCapturer.captureLogs(TestGateway.class);
    rootLogger.addHandler(new ConsoleHandler());

    try {
      Gateway.redirectJulToSlf4j();

      assertThat(rootLogger.getHandlers())
          .singleElement()
          .isInstanceOf(SLF4JBridgeHandler.class);

      String message = "JUL warning redirected to the S3 Gateway log";
      julLogger.warning(message);

      assertThat(logCapturer.getOutput()).contains(message);
    } finally {
      logCapturer.stopCapturing();
    }
  }

  /**
   * A javax filter that cannot be bridged into the Jakarta chain leaves the S3 Gateway web servers
   * unusable, and such a misconfiguration will never succeed on retry, so start-up must abort
   * rather than serve without them.
   *
   * <p>Driven through the real {@link Gateway#call()}: it needs picocli's parse result, so it is
   * reached via {@code execute}, which turns the failure into a non-zero exit code -- the same
   * signal {@code main} terminates on. The stderr text is asserted too, so an unrelated start-up
   * failure cannot make this pass.
   */
  @Test
  void abortsStartupOnNonBridgeableFilter(@TempDir Path metadataDir) {
    Gateway gateway = new Gateway();
    StringWriter stderr = new StringWriter();
    gateway.getCmd().setErr(new PrintWriter(stderr));

    int exitCode = gateway.execute(new String[] {
        "--set", HttpServer2.FILTER_INITIALIZER_PROPERTY + "="
            + TestHttpServer2.NonBridgeableFilterInitializer.class.getName(),
        "--set", OZONE_METADATA_DIRS + "=" + metadataDir});

    assertNotEquals(0, exitCode,
        "S3 Gateway must not start up with a non-bridgeable javax filter");
    assertThat(stderr.toString())
        .as("the failure must name the unbridgeable filter rather than some unrelated error")
        .contains("implements javax.servlet.Filter");
  }

  private void removeRootLoggerHandlers() {
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }
  }
}
