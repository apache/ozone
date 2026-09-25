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

package org.apache.hadoop.ozone.recon.api.filters;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link ReconAuthFilter} on an embedded Jetty EE10 server, which is the only way to cover
 * what it actually does: it hands Recon's own {@code ozone.recon.http.auth.*} configuration to a
 * {@code javax.servlet} {@code ProxyUserAuthenticationFilter} through
 * {@link org.apache.hadoop.hdds.server.http.servletbridge.JavaxFilterBridge}, via a hand-rolled
 * jakarta {@code FilterConfig}. Three things only show up end to end: that the bridged filter
 * initializes off that config at all, that the proxy-user decision -- which reads the request's
 * parameter map and remote address through the bridge -- reaches the same verdict it would on a
 * javax container, and that the effective user it establishes is the one the downstream jakarta
 * chain sees.
 *
 * <p>Simple (pseudo) auth stands in for kerberos: {@code doAs} handling is the same code path for
 * either authentication handler, and it needs no KDC. {@code doAs} is spelled in mixed case, as
 * hadoop clients send it, so the filter's lower-casing wrapper runs -- that reads
 * {@code getParameterMap()} off the bridged request, which the synthetic bridge unit tests never
 * exercise.
 */
public class TestReconAuthFilter {

  private static final String SECRET = "recon-auth-test-secret";

  /** Recon's auth configuration: alice may impersonate, carol is not configured to. */
  private static OzoneConfiguration authConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX + "type", "simple");
    conf.set(OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX + "simple.anonymous.allowed", "false");
    conf.set(OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX + "signature.secret", SECRET);
    conf.set(OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX + "proxyuser.alice.hosts", "*");
    conf.set(OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX + "proxyuser.alice.groups", "*");
    return conf;
  }

  @Test
  public void testProxyUserAuthenticationThroughBridge() throws Exception {
    HttpServer2 server = new HttpServer2.Builder()
        .setConf(authConf())
        // The webapp is only a shell for the filter and servlet: an empty one from the
        // hdds-server-framework test jar, with none of Recon's own Guice-wired web.xml.
        .setName("testing")
        .addEndpoint(URI.create("http://localhost:0"))
        .withoutDefaultApps()
        .build();
    server.addGlobalFilter("recon-auth", ConfiguredReconAuthFilter.class.getName(),
        new HashMap<>());
    server.addServlet("whoami", "/whoami", EffectiveUserServlet.class);
    server.start();
    try {
      String base = "http://localhost:" + server.getConnectorAddress(0).getPort() + "/whoami";

      // No user and anonymous disallowed: the bridged filter refuses the request, which also
      // proves it initialized off Recon's ozone.recon.http.auth.* configuration.
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, statusOf(base));

      // Authenticated, no impersonation: the authenticated user reaches the servlet.
      HttpURLConnection direct = get(base + "?user.name=alice");
      assertEquals(HttpURLConnection.HTTP_OK, direct.getResponseCode());
      assertEquals("alice/alice", readBody(direct),
          "the authenticated user must be the effective user downstream");

      // alice is configured to impersonate, so bob becomes the effective user downstream --
      // both the remote user and the principal, which is what Recon's admin filter reads.
      HttpURLConnection impersonated = get(base + "?user.name=alice&doAs=bob");
      assertEquals(HttpURLConnection.HTTP_OK, impersonated.getResponseCode());
      assertEquals("bob/bob", readBody(impersonated),
          "the impersonated user must replace the authenticated one downstream");

      // carol has no proxyuser configuration, so the impersonation is refused. The 403 the javax
      // filter writes must reach the client instead of the servlet running as bob.
      assertEquals(HttpURLConnection.HTTP_FORBIDDEN, statusOf(base + "?user.name=carol&doAs=bob"));
    } finally {
      server.stop();
    }
  }

  private static HttpURLConnection get(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    return conn;
  }

  private static int statusOf(String url) throws IOException {
    return get(url).getResponseCode();
  }

  private static String readBody(HttpURLConnection conn) throws IOException {
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      return reader.readLine();
    }
  }

  /**
   * Jetty instantiates a filter from its class name with a no-arg constructor, but Recon's filter
   * is Guice-injected with the configuration. This subclass supplies the same configuration the
   * server was built with.
   */
  public static class ConfiguredReconAuthFilter extends ReconAuthFilter {
    public ConfiguredReconAuthFilter() {
      super(authConf());
    }
  }

  /** Echoes the effective user the bridged filter established, as remote user and principal. */
  public static class EffectiveUserServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      resp.setContentType("text/plain");
      resp.getWriter().write(req.getRemoteUser() + "/"
          + (req.getUserPrincipal() == null ? "" : req.getUserPrincipal().getName()));
    }
  }
}
