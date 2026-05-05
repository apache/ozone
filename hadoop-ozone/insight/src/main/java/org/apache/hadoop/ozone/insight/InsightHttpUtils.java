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

package org.apache.hadoop.ozone.insight;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Utility class for making HTTP/HTTPS calls with SPNEGO authentication support.
 */
public final class InsightHttpUtils {

  private InsightHttpUtils() {

  }

  /**
   * Check if SPNEGO authentication is enabled.
   */
  public static boolean isSpnegoEnabled(OzoneConfiguration conf) {
    String authType = conf.get(OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_KEY,
            String.valueOf(OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_DEFAULT));
    return "kerberos".equalsIgnoreCase(authType) || "true".equalsIgnoreCase(authType);
  }

  /**
   * Make an HTTP/HTTPS call with SPNEGO authentication support.
   * 
   * @param url The URL to connect to
   * @param conf The Ozone configuration
   * @return HttpURLConnection or null if connection failed
   * @throws IOException if connection fails
   */
  public static HttpURLConnection openConnection(String url, OzoneConfiguration conf) throws IOException {
    try {
      final URLConnectionFactory connectionFactory =
          URLConnectionFactory.newDefaultURLConnectionFactory(conf);

      boolean isSpnegoEnabled = isSpnegoEnabled(conf);

      return (HttpURLConnection)
          connectionFactory.openConnection(new URL(url), isSpnegoEnabled);
    } catch (ConnectException ex) {
      System.err.println("Connection Refused: " + url);
      return null;
    } catch (AuthenticationException authEx) {
      System.err.println("Authentication Failed. Please make sure you " +
          "have logged in with kinit or disable Ozone security settings.");
      return null;
    } catch (Exception ex) {
      throw new IOException("Failed to connect to " + url, ex);
    }
  }

  /**
   * Read the response from an HttpURLConnection as a String.
   */
  public static String readResponse(HttpURLConnection httpURLConnection) throws IOException {
    if (httpURLConnection == null) {
      return null;
    }
    
    int responseCode = httpURLConnection.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + responseCode + ": " + httpURLConnection.getResponseMessage());
    }
    
    try (InputStream inputStream = httpURLConnection.getInputStream();
         InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
         BufferedReader bufferedReader = new BufferedReader(reader)) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }

  /**
   * Read response as a stream of lines (for streaming endpoints like /logstream).
   */
  public static BufferedReader getResponseReader(HttpURLConnection httpURLConnection) throws IOException {
    if (httpURLConnection == null) {
      return null;
    }
    
    int responseCode = httpURLConnection.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + responseCode + ": " + httpURLConnection.getResponseMessage());
    }
    
    return new BufferedReader(new InputStreamReader(
        httpURLConnection.getInputStream(), StandardCharsets.UTF_8));
  }
}

