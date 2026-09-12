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

package org.apache.ozone.fs.http.server;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HttpFSServerWebServer}.
 */
public class TestHttpFSServerWebServer {

  /**
   * WebHDFS paths (/webhdfs/v1/&lt;path&gt;) carry user file names that can
   * contain characters ('%' arrives as %25) or empty segments ("//") which
   * Jetty 12 rejects with 400 by default. The HttpFS web server must allow such
   * ambiguous URIs, which puts the connector in LEGACY URI compliance mode.
   * {@code TestHttpServer2} covers that LEGACY mode also decodes ambiguous URIs
   * in the servlet layer.
   */
  @Test
  public void allowsAmbiguousUris() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    HttpFSServerWebServer webServer =
        new HttpFSServerWebServer(conf, new Configuration(false));

    ServerConnector connector = (ServerConnector) jettyServer(webServer)
        .getConnectors()[0];
    assertSame(UriCompliance.LEGACY,
        connector.getConnectionFactory(HttpConnectionFactory.class)
            .getHttpConfiguration().getUriCompliance());
  }

  private static Server jettyServer(HttpFSServerWebServer webServer)
      throws ReflectiveOperationException {
    Field httpServerField =
        HttpFSServerWebServer.class.getDeclaredField("httpServer");
    httpServerField.setAccessible(true);
    HttpServer2 httpServer = (HttpServer2) httpServerField.get(webServer);

    Field webServerField = HttpServer2.class.getDeclaredField("webServer");
    webServerField.setAccessible(true);
    return (Server) webServerField.get(httpServer);
  }
}
