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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.security.KeyStore;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for HttpServer2 SSL/TLS configuration: cipher suite filtering
 * and protocol version enforcement.
 */
public class TestHttpServer2SSL {

  @TempDir
  private static File baseDir;
  private static String keystoresDir;
  private static String sslConfDir;
  private static OzoneConfiguration conf;
  private static Configuration sslServerConf;

  @BeforeAll
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestHttpServer2SSL.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    // Read back the generated keystore/truststore properties
    sslServerConf = new Configuration(false);
    sslServerConf.addResource(KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Test
  public void testExcludedCiphers() throws Exception {
    // Pick a cipher that's normally available and exclude it
    String excludedCipher = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
    HttpServer2 server = buildServer(excludedCipher, null, null);
    server.start();
    try {
      InetSocketAddress addr = server.getConnectorAddress(0);
      // Connect with only the excluded cipher — should fail
      SSLSocketFactory factory = createSocketFactory(new String[]{excludedCipher}, null);
      assertThrows(SSLHandshakeException.class, () -> connectWithFactory(factory, addr));
    } finally {
      server.stop();
    }
  }

  @Test
  public void testIncludedCiphers() throws Exception {
    // Include only one specific cipher
    String includedCipher = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
    HttpServer2 server = buildServer(null, includedCipher, null);
    server.start();
    try {
      InetSocketAddress addr = server.getConnectorAddress(0);
      // Connect with the included cipher — should succeed
      SSLSocketFactory factory = createSocketFactory(new String[]{includedCipher}, null);
      int responseCode = connectWithFactory(factory, addr);
      assertEquals(HttpURLConnection.HTTP_OK, responseCode);
    } finally {
      server.stop();
    }
  }

  @Test
  public void testIncludedCiphersRejectsOther() throws Exception {
    // Include only one cipher; try connecting with a different one
    String includedCipher = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
    String otherCipher = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
    HttpServer2 server = buildServer(null, includedCipher, null);
    server.start();
    try {
      InetSocketAddress addr = server.getConnectorAddress(0);
      SSLSocketFactory factory = createSocketFactory(new String[]{otherCipher}, null);
      assertThrows(SSLHandshakeException.class, () -> connectWithFactory(factory, addr));
    } finally {
      server.stop();
    }
  }

  @Test
  public void testEnabledProtocolsTls12() throws Exception {
    HttpServer2 server = buildServer(null, null, "TLSv1.2");
    server.start();
    try {
      InetSocketAddress addr = server.getConnectorAddress(0);
      // Connect with TLSv1.2 — should succeed
      SSLSocketFactory factory = createSocketFactory(null, new String[]{"TLSv1.2"});
      int responseCode = connectWithFactory(factory, addr);
      assertEquals(HttpURLConnection.HTTP_OK, responseCode);
    } finally {
      server.stop();
    }
  }

  @Test
  public void testEnabledProtocolsRejectsOther() throws Exception {
    // Configure server to accept only TLSv1.2, try connecting with TLSv1.1
    HttpServer2 server = buildServer(null, null, "TLSv1.2");
    server.start();
    try {
      InetSocketAddress addr = server.getConnectorAddress(0);
      SSLSocketFactory factory = createSocketFactory(null, new String[]{"TLSv1.1"});
      assertThrows(Exception.class, () -> connectWithFactory(factory, addr));
    } finally {
      server.stop();
    }
  }

  @Test
  public void testDefaultConfigAcceptsConnection() throws Exception {
    // No cipher/protocol restrictions — connection should succeed
    HttpServer2 server = buildServer(null, null, null);
    server.start();
    try {
      InetSocketAddress addr = server.getConnectorAddress(0);
      SSLSocketFactory factory = createSocketFactory(null, null);
      int responseCode = connectWithFactory(factory, addr);
      assertEquals(HttpURLConnection.HTTP_OK, responseCode);
    } finally {
      server.stop();
    }
  }

  private HttpServer2 buildServer(String excludeCiphers, String includeCiphers, String enabledProtocols)
      throws Exception {
    OzoneConfiguration serverConf = new OzoneConfiguration(conf);
    if (enabledProtocols != null) {
      serverConf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, enabledProtocols);
    }
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("test")
        .addEndpoint(new URI("https://localhost:0"))
        .setConf(serverConf)
        .keyStore(
            sslServerConf.get(SSLFactory.SSL_SERVER_KEYSTORE_LOCATION),
            sslServerConf.get(SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD),
            sslServerConf.get(SSLFactory.SSL_SERVER_KEYSTORE_TYPE, SSLFactory.SSL_SERVER_KEYSTORE_TYPE_DEFAULT))
        .trustStore(
            sslServerConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_LOCATION),
            sslServerConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_PASSWORD),
            sslServerConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_TYPE, SSLFactory.SSL_SERVER_TRUSTSTORE_TYPE_DEFAULT))
        .keyPassword(
            sslServerConf.get(SSLFactory.SSL_SERVER_KEYSTORE_KEYPASSWORD));
    if (excludeCiphers != null) {
      builder.excludeCiphers(excludeCiphers);
    }
    if (includeCiphers != null) {
      builder.includeCiphers(includeCiphers);
    }
    return builder.build();
  }

  /**
   * Creates an SSLSocketFactory that wraps the test trust store
   * and can force specific ciphers and/or protocols on created sockets.
   */
  private SSLSocketFactory createSocketFactory(String[] ciphers, String[] protocols) throws Exception {
    // Trust store was created by KeyStoreTestUtil.setupSSLConfig
    String tsLocation = sslServerConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_LOCATION);
    String tsPassword = sslServerConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_PASSWORD);
    KeyStore ts = KeyStore.getInstance("jks");
    try (InputStream in = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(tsLocation))) {
      ts.load(in, tsPassword != null ? tsPassword.toCharArray() : null);
    }
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    TrustManager[] trustManagers = tmf.getTrustManagers();

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagers, null);
    SSLSocketFactory baseFactory = sslContext.getSocketFactory();

    if (ciphers == null && protocols == null) {
      return baseFactory;
    }
    return new ConstrainedSSLSocketFactory(baseFactory, ciphers, protocols);
  }

  /**
   * Connects to the HTTPS server using the given SSLSocketFactory
   * and returns the HTTP response code.
   */
  private int connectWithFactory(SSLSocketFactory factory, InetSocketAddress addr) throws IOException {
    URL url = new URL("https://localhost:" + addr.getPort() + "/jmx");
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(factory);
    conn.setHostnameVerifier((hostname, session) -> true);
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    return conn.getResponseCode();
  }

  /**
   * An SSLSocketFactory wrapper that constrains the cipher suites
   * and/or protocols on created sockets.
   */
  private static class ConstrainedSSLSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegate;
    private final String[] ciphers;
    private final String[] protocols;

    ConstrainedSSLSocketFactory(SSLSocketFactory delegate, String[] ciphers, String[] protocols) {
      this.delegate = delegate;
      this.ciphers = ciphers;
      this.protocols = protocols;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return ciphers != null ? ciphers : delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return ciphers != null ? ciphers : delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
      return constrain((SSLSocket) delegate.createSocket(s, host, port, autoClose));
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return constrain((SSLSocket) delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(String host, int port, java.net.InetAddress localHost, int localPort)
        throws IOException {
      return constrain((SSLSocket) delegate.createSocket(host, port, localHost, localPort));
    }

    @Override
    public Socket createSocket(java.net.InetAddress host, int port) throws IOException {
      return constrain((SSLSocket) delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(java.net.InetAddress address, int port, java.net.InetAddress localAddress, int localPort)
        throws IOException {
      return constrain((SSLSocket) delegate.createSocket(address, port, localAddress, localPort));
    }

    private SSLSocket constrain(SSLSocket socket) {
      SSLParameters params = socket.getSSLParameters();
      if (ciphers != null) {
        params.setCipherSuites(ciphers);
      }
      if (protocols != null) {
        params.setProtocols(protocols);
      }
      socket.setSSLParameters(params);
      return socket;
    }
  }
}
