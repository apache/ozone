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

package org.apache.hadoop.ozone.admin.nssummary;

import static org.apache.hadoop.ozone.admin.nssummary.NSSummaryCLIUtils.printWithUnderline;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import javax.security.sasl.AuthenticationException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import picocli.CommandLine;

/**
 * Namespace Summary Rebuild Subcommand.
 */
@CommandLine.Command(
    name = "rebuild",
    description = "Rebuild the Recon Namespace Summary information.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class RebuildSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private NSSummaryAdmin parent;

  private static final String ENDPOINT = "/api/v1/namespace/rebuild";

  @Override
  public Void call() throws Exception {
    StringBuilder url = new StringBuilder();
    url.append(parent.getReconWebAddress()).append(ENDPOINT);

    System.out.println("Connecting to Recon: " + url + " ...");
    
    final URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(
            (Configuration) parent.getOzoneConfig());

    HttpURLConnection httpURLConnection;

    try {
      httpURLConnection = (HttpURLConnection)
          connectionFactory.openConnection(new URL(url.toString()),
              parent.isHTTPSEnabled());
      httpURLConnection.setRequestMethod("POST");
      httpURLConnection.setRequestProperty("X-Requested-With", "OzoneAdminCLI");
      httpURLConnection.setDoOutput(true);
      httpURLConnection.connect();
      
      int responseCode = httpURLConnection.getResponseCode();
      String response = null;
      
      if (responseCode >= 200 && responseCode < 300) {
        response = IOUtils.toString(httpURLConnection.getInputStream(), StandardCharsets.UTF_8);
      } else if (httpURLConnection.getErrorStream() != null) {
        response = IOUtils.toString(httpURLConnection.getErrorStream(), StandardCharsets.UTF_8);
      }

      handleResponse(responseCode, response);
      
    } catch (ConnectException ex) {
      System.err.println("Connection Refused. Please make sure the " +
          "Recon Server has been started.");
    } catch (AuthenticationException authEx) {
      System.err.println("Authentication Failed. Please make sure you " +
          "have login or disable Ozone security settings.");
    }

    return null;
  }

  private void handleResponse(int responseCode, String response) {
    try {
      if (response != null && !response.isEmpty()) {
        JsonNode jsonResponse = JsonUtils.readTree(response);
        String status = jsonResponse.path("status").asText();
        String message = jsonResponse.path("message").asText();

        System.out.println();
        
        switch (status) {
          case "STARTED":
            printWithUnderline("Namespace Summary rebuild initiated.", true);
            System.out.println("Existing data will be cleared and the tree will be rebuilt. " +
                "This may take some time to complete.");
            break;
          case "IN_PROGRESS":
            printWithUnderline("Namespace Summary rebuild is already in progress.", true);
            System.out.println("Please wait for the current rebuild to finish before " +
                "starting a new one.");
            break;
          case "RECON_INITIALIZING":
            System.out.println("Recon is being initialized. Please wait a moment.");
            break;
          case "FORBIDDEN":
            System.err.println("Access denied: " + message);
            break;
          default:
            System.err.println("Unexpected response: " + message);
            break;
        }
      } else {
        handleErrorResponse(responseCode);
      }
    } catch (Exception e) {
      System.err.println("Failed to parse response: " + e.getMessage());
      if (response != null) {
        System.err.println("Raw response: " + response);
      }
    }
  }

  private void handleErrorResponse(int responseCode) {
    switch (responseCode) {
      case 503:
        System.out.println("Recon is being initialized. Please wait a moment.");
        break;
      case 500:
        System.err.println("Internal server error occurred while triggering rebuild.");
        break;
      default:
        System.err.println("Unexpected response code: " + responseCode);
        break;
    }
  }
}
