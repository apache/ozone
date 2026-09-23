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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ozone.RootedOzoneFileSystem;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that a "doas" request through HttpFS reaches Ozone as the impersonated user.
 *
 * <p>HttpFS decides impersonation inside its authentication filter, which is a
 * {@code javax.servlet.Filter} run through {@code JavaxFilterBridge}: the filter validates the
 * proxy user, wraps its javax request with the impersonated identity and forwards the wrapper, and
 * the bridge overlays that identity onto the jakarta request before continuing the chain. The
 * sibling unit test in {@code TestHttpFSServerWebServer} only checks HTTP outcomes, and is backed
 * by {@code file:///} whose ownership comes from the JVM's own user, so it cannot show which user
 * actually reached the filesystem.
 *
 * <p>This test reads the effective user out of OM. HttpFS always talks to OM as a proxy for the
 * request's user, and this cluster authorizes no impersonation at all, so every request is refused
 * by OM -- but the refusal names the user HttpFS proxied for. That name is the assertion: without
 * {@code doas} it is the authenticated user, with {@code doas} it is the impersonated one. Denied
 * impersonation never reaches OM, because the bridged filter short-circuits first.
 *
 * <p>Asserting through the refusal rather than a created object's owner keeps the test free of
 * cluster-side proxy-user configuration, which is JVM-global state in Hadoop's {@code ProxyUsers}
 * and does not belong in a single test's setup.
 */
public class TestHttpFSImpersonation {

  private static final String REAL_USER = "alice";
  private static final String DOAS_USER = "bob";
  private static final String DENIED_USER = "carol";

  @Test
  public void doAsUserReachesOzoneAsImpersonatedUser(@TempDir Path baseDir) throws Exception {
    Map<String, String> savedProperties = new HashMap<>();
    // MKDIRS is an OM metadata operation, so no datanodes are needed.
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration())
        .withoutDatanodes()
        .build();
    HttpFSServerWebServer webServer = null;
    try {
      cluster.waitForClusterToBeReady();
      final String volume = "vol" + UUID.randomUUID().toString().substring(0, 8);
      final String bucket = "buck" + UUID.randomUUID().toString().substring(0, 8);
      try (OzoneClient client = cluster.newClient()) {
        client.getObjectStore().createVolume(volume);
        client.getObjectStore().getVolume(volume).createBucket(bucket);
      }

      webServer = startHttpFs(baseDir, cluster, savedProperties);
      final String base = webServer.getUrl().toString() + "/v1/" + volume + "/" + bucket + "/";

      // Without doas, HttpFS proxies for the authenticated user, so OM names alice.
      assertThat(refusedUserOf(base + "plain?op=MKDIRS&user.name=" + REAL_USER))
          .as("OM must see the authenticated user when no impersonation is requested")
          .contains("impersonate " + REAL_USER);

      // With doas, the bridged filter replaces the identity it forwards, the bridge carries that
      // through, and HttpFS proxies for bob instead -- which is what OM now names.
      String refusal = refusedUserOf(
          base + "doas?op=MKDIRS&user.name=" + REAL_USER + "&doas=" + DOAS_USER);
      assertThat(refusal)
          .as("OM must see the impersonated user, proving doas survived the javax/jakarta bridge")
          .contains("impersonate " + DOAS_USER);
      assertThat(refusal)
          .as("the authenticated user must no longer be the effective user")
          .doesNotContain("impersonate " + REAL_USER);

      // Denied impersonation: carol is not a configured proxy user, so the bridged filter
      // short-circuits with 403 and the jakarta chain -- and therefore OM -- is never reached.
      assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          open(base + "denied?op=MKDIRS&user.name=" + DENIED_USER + "&doas=" + DOAS_USER, "PUT")
              .getResponseCode());
    } finally {
      if (webServer != null) {
        webServer.stop();
      }
      restore(savedProperties);
      cluster.shutdown();
    }
  }

  /**
   * Issues a MKDIRS that OM is expected to refuse, and returns the error body carrying OM's
   * impersonation refusal (HttpFS renders it as a WebHDFS RemoteException).
   */
  private static String refusedUserOf(String url) throws Exception {
    HttpURLConnection conn = open(url, "PUT");
    assertNotEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode(),
        "this cluster authorizes no impersonation, so the operation must be refused");
    return readBody(conn);
  }

  /**
   * Starts an embedded HttpFS backed by the cluster. HttpFS reads its filesystem settings from
   * {@code core-site.xml} in its config directory (not from the constructor's configuration, whose
   * second argument is the SSL configuration), so the cluster's own configuration is written there
   * with the Ozone root filesystem as the default.
   */
  private static HttpFSServerWebServer startHttpFs(Path baseDir, MiniOzoneCluster cluster,
      Map<String, String> savedProperties) throws Exception {
    Path confDir = Files.createDirectories(baseDir.resolve("conf"));
    Path secretFile = confDir.resolve("httpfs-signature.secret");
    Files.write(secretFile, "impersonation-test-secret".getBytes(UTF_8));

    OzoneConfiguration fsConf = new OzoneConfiguration(cluster.getConf());
    fsConf.set("fs.defaultFS", String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, cluster.getConf().get(OZONE_OM_ADDRESS_KEY)));
    fsConf.set("fs.ofs.impl", RootedOzoneFileSystem.class.getName());
    try (OutputStream out = Files.newOutputStream(confDir.resolve("core-site.xml"))) {
      fsConf.writeXml(out);
    }

    Files.write(confDir.resolve("httpfs-site.xml"), (
        "<?xml version=\"1.0\"?>\n"
        + "<configuration>\n"
        + property("hadoop.http.authentication.simple.anonymous.allowed", "false")
        + property("hadoop.http.authentication.signature.secret.file", secretFile.toString())
        + property("httpfs.hadoop.name.node.whitelist", "*")
        // Only alice may impersonate; carol is deliberately left unconfigured.
        + property("httpfs.proxyuser." + REAL_USER + ".hosts", "*")
        + property("httpfs.proxyuser." + REAL_USER + ".groups", "*")
        + "</configuration>\n").getBytes(UTF_8));

    setProperty(savedProperties, "httpfs.home.dir", baseDir.toString());
    setProperty(savedProperties, "httpfs.config.dir", confDir.toString());
    setProperty(savedProperties, "httpfs.log.dir",
        Files.createDirectories(baseDir.resolve("log")).toString());
    setProperty(savedProperties, "httpfs.temp.dir",
        Files.createDirectories(baseDir.resolve("temp")).toString());

    OzoneConfiguration serverConf = new OzoneConfiguration();
    serverConf.set("httpfs.http.hostname", "localhost");
    serverConf.setInt("httpfs.http.port", 0);
    HttpFSServerWebServer server =
        new HttpFSServerWebServer(serverConf, new Configuration(false));
    server.start();
    return server;
  }

  private static String property(String name, String value) {
    return "  <property>\n"
        + "    <name>" + name + "</name>\n"
        + "    <value>" + value + "</value>\n"
        + "  </property>\n";
  }

  private static HttpURLConnection open(String url, String method) throws Exception {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod(method);
    conn.setInstanceFollowRedirects(false);
    return conn;
  }

  /** Reads the response body, falling back to the error stream for a failed request. */
  private static String readBody(HttpURLConnection conn) throws Exception {
    InputStream in = conn.getErrorStream() != null
        ? conn.getErrorStream() : conn.getInputStream();
    StringBuilder body = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        body.append(line);
      }
    }
    return body.toString();
  }

  private static void setProperty(Map<String, String> saved, String name, String value) {
    saved.put(name, System.getProperty(name));
    System.setProperty(name, value);
  }

  private static void restore(Map<String, String> saved) {
    for (Map.Entry<String, String> entry : saved.entrySet()) {
      if (entry.getValue() == null) {
        System.clearProperty(entry.getKey());
      } else {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }
}
