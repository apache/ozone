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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.hdds.server.http.TestHttpServer2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests Recon server startup behavior.
 */
public class TestReconServer {

  /**
   * A javax filter that cannot be bridged into the Jakarta chain leaves the Recon web server
   * unusable, and such a misconfiguration will never succeed on retry, so Recon must abort rather
   * than keep running with no web server.
   *
   * <p>Driven through the real {@link ReconServer#call()}: it needs picocli's parse result, so it
   * is reached via {@code execute}, which turns the failure into a non-zero exit code -- the same
   * signal {@code main} terminates on. The stderr text is asserted too, so an unrelated start-up
   * failure (a missing database directory, say) cannot make this pass.
   */
  @Test
  void abortsStartupOnNonBridgeableFilter(@TempDir Path dir) {
    ReconServer reconServer = new ReconServer();
    StringWriter stderr = new StringWriter();
    reconServer.getCmd().setErr(new PrintWriter(stderr));

    int exitCode = reconServer.execute(new String[] {
        "--set", OZONE_METADATA_DIRS + "=" + dir,
        "--set", OZONE_RECON_DB_DIR + "=" + dir,
        "--set", OZONE_RECON_OM_SNAPSHOT_DB_DIR + "=" + dir,
        "--set", OZONE_RECON_SCM_DB_DIR + "=" + dir,
        "--set", HttpServer2.FILTER_INITIALIZER_PROPERTY + "="
            + TestHttpServer2.NonBridgeableFilterInitializer.class.getName()});

    assertNotEquals(0, exitCode,
        "Recon must not start up with a non-bridgeable javax filter");
    assertThat(stderr.toString())
        .as("the failure must name the unbridgeable filter rather than some unrelated error")
        .contains("implements javax.servlet.Filter");
  }
}
