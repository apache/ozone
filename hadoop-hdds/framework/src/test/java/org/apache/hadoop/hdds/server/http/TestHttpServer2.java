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

package org.apache.hadoop.hdds.server.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.Test;

/**
 * Testing HttpServer2.
 */
public class TestHttpServer2 {

  /**
   * Test hadoop.http.idle_timeout.ms correctly loaded, and not being default
   * value from core-default.xml of hadoop-common.
   *
   * @throws Exception
   */
  @Test
  public void testIdleTimeout() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    URI uri = URI.create("https://example.com/");

    HttpServer2 srv = new HttpServer2.Builder()
            .setConf(conf)
            .setName("test")
            .addEndpoint(uri)
            .build();
    for (ServerConnector server : srv.getListeners()) {
      // Check default value in ozone-default.xml
      assertEquals(60000, server.getIdleTimeout());
    }
  }

  /**
   * When ozone.http.basedir is not set, the base dir must be created under the
   * system temporary directory (java.io.tmpdir), not the process working
   * directory, so startup does not fail when the CWD is not writable.
   */
  @Test
  public void testSetHttpBaseDirUsesSystemTempDir() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.unset(OzoneConfigKeys.OZONE_HTTP_BASEDIR);

    HttpServer2.setHttpBaseDir(conf);

    String baseDir = conf.get(OzoneConfigKeys.OZONE_HTTP_BASEDIR);
    assertThat(baseDir).isNotEmpty();
    Path baseDirPath = Paths.get(baseDir).toAbsolutePath().normalize();
    assertThat(baseDirPath).exists();
    Path systemTmpDir = Paths.get(System.getProperty("java.io.tmpdir"))
        .toAbsolutePath().normalize();
    assertThat(baseDirPath.startsWith(systemTmpDir)).isTrue();
  }
}
