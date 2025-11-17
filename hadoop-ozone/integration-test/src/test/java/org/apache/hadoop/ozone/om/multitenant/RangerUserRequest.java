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

package org.apache.hadoop.ozone.om.multitenant;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.kerby.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to create and delete users in Ranger because RangerClient
 * doesn't support it yet.
 */
public class RangerUserRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(RangerUserRequest.class);

  private String rangerEndpoint;

  // Value of HTTP auth header
  private String authHeaderValue;

  private int connectionTimeout = 5000;
  private int connectionRequestTimeout = 5000;

  public RangerUserRequest(String rangerHttpsAddress, String userName,
                           String passwd) {

    // Trim trailing slash
    if (rangerHttpsAddress.endsWith("/")) {
      rangerHttpsAddress =
          rangerHttpsAddress.substring(0, rangerHttpsAddress.length() - 1);
    }
    this.rangerEndpoint = rangerHttpsAddress;

    String auth = userName + ":" + passwd;
    byte[] encodedAuth =
        Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
    authHeaderValue = "Basic " +
        new String(encodedAuth, StandardCharsets.UTF_8);

    setupRangerIgnoreServerCertificate();
  }

  private void setupRangerIgnoreServerCertificate() {
    // Create a trust manager that does not validate certificate chains
    TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          @Override
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
          }

          @Override
          public void checkClientTrusted(
              java.security.cert.X509Certificate[] certs, String authType) {
          }

          @Override
          public void checkServerTrusted(
              java.security.cert.X509Certificate[] certs, String authType) {
          }
        }
    };

    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    } catch (Exception e) {
      LOG.info("Setting DefaultSSLSocketFactory failed.");
    }
  }

  private HttpURLConnection openURLConnection(URL url) throws IOException {
    final HttpURLConnection urlConnection;
    if (url.getProtocol().equals("https")) {
      urlConnection = (HttpsURLConnection) url.openConnection();
    } else if (url.getProtocol().equals("http")) {
      urlConnection = (HttpURLConnection) url.openConnection();
    } else {
      throw new IOException("Unsupported protocol: " + url.getProtocol() +
          "URL: " + url);
    }
    return urlConnection;
  }

  /**
   * Can make either http or https request.
   */
  private HttpURLConnection makeHttpCall(String urlString,
      String jsonInputString, String method, boolean isSpnego)
      throws IOException {

    URL url = new URL(urlString);
    final HttpURLConnection urlConnection = openURLConnection(url);

    urlConnection.setRequestMethod(method);
    // Timeout in ms
    urlConnection.setConnectTimeout(connectionTimeout);
    // Timeout in ms
    urlConnection.setReadTimeout(connectionRequestTimeout);
    urlConnection.setRequestProperty("Accept", "application/json");
    urlConnection.setRequestProperty("Authorization", authHeaderValue);

    if ((jsonInputString != null) && !jsonInputString.isEmpty()) {
      urlConnection.setDoOutput(true);
      urlConnection.setRequestProperty("Content-Type", "application/json;");
      try (OutputStream os = urlConnection.getOutputStream()) {
        byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
        os.flush();
      }
    }

    return urlConnection;
  }

  private String getCreateUserJsonStr(String userName, String password) {
    return "{"
        + "  \"name\":\"" +  userName + "\","
        + "  \"password\":\"" +  password + "\","
        + "  \"firstName\":\"" +  userName + "\","
        + "  \"userRoleList\":[\"ROLE_USER\"]"
        + "}";
  }

  private String getResponseData(HttpURLConnection urlConnection)
      throws IOException {
    StringBuilder response = new StringBuilder();
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(urlConnection.getInputStream(),
            StandardCharsets.UTF_8))) {
      String responseLine;
      while ((responseLine = br.readLine()) != null) {
        response.append(responseLine.trim());
      }
      LOG.debug("Got response: {}", response);
      // TODO: throw if urlConnection code is 400?
    } catch (IOException e) {
      // Common exceptions:
      // 1. Server returned HTTP response code: 401
      //   - Possibly incorrect Ranger credentials
      // 2. Server returned HTTP response code: 400
      //   - Policy or role does not exist
      switch (urlConnection.getResponseCode()) {
      case 400:
        LOG.error("The policy or role likely does not exist in Ranger");
        return null;
      case 401:
        LOG.error("Check Ranger credentials");
        //        break;
      default:
        e.printStackTrace();
        throw e;
      }
    }
    return response.toString();
  }

  private HttpURLConnection makeHttpGetCall(String urlString,
                                            String method, boolean isSpnego) throws IOException {

    URL url = new URL(urlString);
    final HttpURLConnection urlConnection = openURLConnection(url);

    urlConnection.setRequestMethod(method);
    urlConnection.setConnectTimeout(connectionTimeout);
    urlConnection.setReadTimeout(connectionRequestTimeout);
    urlConnection.setRequestProperty("Accept", "application/json");
    urlConnection.setRequestProperty("Authorization", authHeaderValue);

    return urlConnection;
  }

  public String getUserId(String userPrincipal) throws IOException {
    String rangerAdminUrl =
        rangerEndpoint + OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT +
            userPrincipal;

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl,
        "GET", false);
    String response = getResponseData(conn);
    String userIDCreated = null;
    try {
      JsonNode jResponse =
          JsonUtils.readTree(response);
      JsonNode userinfo = jResponse.path("vXUsers");
      int numIndex = userinfo.size();

      for (int i = 0; i < numIndex; ++i) {
        JsonNode userNode = userinfo.get(i);
        String name = userNode.path("name").asText();
        if (name.equals(userPrincipal)) {
          userIDCreated = userNode.path("id").asText();
          break;
        }
      }
      LOG.debug("User ID is: {}", userIDCreated);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }

    return userIDCreated;
  }

  public String createUser(String userName, String password)
      throws IOException {

    String endpointUrl =
        rangerEndpoint + OZONE_OM_RANGER_ADMIN_CREATE_USER_HTTP_ENDPOINT;

    String jsonData = getCreateUserJsonStr(userName, password);

    final HttpURLConnection conn = makeHttpCall(endpointUrl,
        jsonData, "POST", false);
    if (conn.getResponseCode() != HTTP_OK) {
      throw new IOException("Ranger REST API failure: " + conn.getResponseCode()
          + " " + conn.getResponseMessage()
          + ". User name '" + userName + "' likely already exists in Ranger");
    }
    String userInfo = getResponseData(conn);
    String userId;
    try {
      assert userInfo != null;
      JsonNode jNode = JsonUtils.readTree(userInfo);
      userId = jNode.get("id").asText();
      LOG.debug("Ranger returned userId: {}", userId);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return userId;
  }

  public void deleteUser(String userId) throws IOException {

    String rangerAdminUrl =
        rangerEndpoint + OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT
            + userId + "?forceDelete=true";

    HttpURLConnection conn = makeHttpCall(rangerAdminUrl, null,
        "DELETE", false);
    int respnseCode = conn.getResponseCode();
    if (respnseCode != 200 && respnseCode != 204) {
      throw new IOException("Couldn't delete user " + userId);
    }
  }
}
