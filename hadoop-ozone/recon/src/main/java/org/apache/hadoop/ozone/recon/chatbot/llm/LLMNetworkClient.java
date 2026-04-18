/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.chatbot.llm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * A shared network utility class for sending HTTP requests to AI providers.
 * 
 * Purpose:
 * Instead of abstract classes and complicated inheritance, this is a simple utility wrapper.
 * The specific AI clients (like OpenAIClient) instantiate this class and use it to execute their network requests,
 * cleanly separating the "logic of JSON formatting" from the "logic of sending bytes over the internet".
 */
public class LLMNetworkClient {

  private static final Logger LOG = LoggerFactory.getLogger(LLMNetworkClient.class);
  private final int timeoutMs;

  public LLMNetworkClient(int timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  /**
   * THE MAIN ENGINE. 
   * Opens an HTTP connection, sends a JSON string, and returns the response JSON string.
   */
  public String executePost(String urlString, Map<String, String> headers, String jsonBody, String providerName) throws LLMClient.LLMException {
    HttpURLConnection conn = null;
    try {
      // Step 1: Open the connection
      conn = (HttpURLConnection) new URL(urlString).openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true); // Allows us to send data IN the body
      conn.setConnectTimeout(timeoutMs); // How long to wait to plug in the initial cable
      conn.setReadTimeout(timeoutMs);    // How long to wait for the AI to type out its answer
      conn.setRequestProperty("Content-Type", "application/json");

      // Step 2: Inject any custom headers (like API keys or version strings)
      if (headers != null) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          conn.setRequestProperty(entry.getKey(), entry.getValue());
        }
      }

      // Step 3: Shove our constructed JSON String through the connection tube
      try (OutputStream os = conn.getOutputStream()) {
        os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
        os.flush();
      }

      // Step 4: Fire the request over the internet and wait for the code!
      int statusCode = conn.getResponseCode();

      // Step 5: Check if the AI responded happily (Status 200 = OK)
      if (statusCode == 200) {
        return readResponse(conn);
      } else {
        // If the AI crashed or returned an error (like 401 Unauthorized)
        String errorMsg = readErrorResponse(conn);
        String formattedError = String.format("%s request failed with status %d: %s", providerName, statusCode, errorMsg);
        LOG.error(formattedError);
        throw new LLMClient.LLMException(formattedError, statusCode);
      }
    } catch (IOException e) {
      LOG.error("Failed to communicate with {}", providerName, e);
      throw new LLMClient.LLMException("Failed to communicate with " + providerName + ": " + e.getMessage(), e);
    } finally {
      // Step 6: Cleanup. Always close the internet connection so we don't leak memory.
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * Reads a successful response from the AI, line by line, until it's finished.
   */
  private String readResponse(HttpURLConnection conn) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    }
    return sb.toString();
  }

  /**
   * Reads an error response from the AI (like "Invalid API Key"). 
   * Networks use a separate "Error Stream" from the "Input Stream" when something goes wrong!
   */
  private String readErrorResponse(HttpURLConnection conn) {
    try {
      if (conn.getErrorStream() != null) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = br.readLine()) != null) {
            sb.append(line);
          }
        }
        return sb.toString();
      }
    } catch (IOException e) {
      LOG.debug("Failed to read error stream", e);
    }
    return "";
  }
}
