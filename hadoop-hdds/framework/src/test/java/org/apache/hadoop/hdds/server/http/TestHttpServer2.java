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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.HttpConnectionFactory;
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
   * By default ambiguous URIs (e.g. empty path segments from "//") are rejected
   * with a 400: the connector uses Jetty's strict URI compliance and the
   * servlet layer does not decode ambiguous URIs.
   */
  @Test
  public void testUriComplianceStrictByDefault() throws Exception {
    HttpServer2 srv = buildServer(false);
    assertSame(UriCompliance.DEFAULT, uriComplianceOf(srv));
    assertFalse(srv.getWebAppContext().getServletHandler()
        .isDecodeAmbiguousURIs());
  }

  /**
   * With allowAmbiguousUri the connector uses the LEGACY compliance mode and
   * the servlet layer decodes ambiguous URIs, accepting empty path segments as
   * Jetty 9.4 did. The S3 Gateway needs this for object keys containing "//".
   */
  @Test
  public void testUriComplianceLegacyWhenAmbiguousAllowed() throws Exception {
    HttpServer2 srv = buildServer(true);
    assertSame(UriCompliance.LEGACY, uriComplianceOf(srv));
    assertTrue(srv.getWebAppContext().getServletHandler()
        .isDecodeAmbiguousURIs());
  }

  private static HttpServer2 buildServer(boolean allowAmbiguousUri)
      throws Exception {
    return new HttpServer2.Builder()
        .setConf(new OzoneConfiguration())
        .setName("test")
        .addEndpoint(URI.create("http://example.com/"))
        .allowAmbiguousUri(allowAmbiguousUri)
        .build();
  }

  private static UriCompliance uriComplianceOf(HttpServer2 srv) {
    ServerConnector connector = srv.getListeners().get(0);
    return connector.getConnectionFactory(HttpConnectionFactory.class)
        .getHttpConfiguration().getUriCompliance();
  }
}
