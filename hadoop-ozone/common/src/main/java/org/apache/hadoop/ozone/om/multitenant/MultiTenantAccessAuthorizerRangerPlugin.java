/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.multitenant;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_ROLE_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_GROUP_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_ROLE_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_ROLE_ADD_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT_DEFAULT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.http.auth.BasicUserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements MultiTenantAccessAuthorizer for Apache Ranger.
 */
public class MultiTenantAccessAuthorizerRangerPlugin implements
    MultiTenantAccessAuthorizer {
  public static final Logger LOG = LoggerFactory
      .getLogger(MultiTenantAccessAuthorizerRangerPlugin.class);

  private OzoneConfiguration conf;
  private boolean ignoreServerCert = false;
  private int connectionTimeout;
  private int connectionRequestTimeout;
  private String authHeaderValue;
  private String rangerHttpsAddress;

  @Override
  public void init(Configuration configuration) throws IOException {
    conf = new OzoneConfiguration(configuration);
    rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    initializeRangerConnection();
  }

  private void initializeRangerConnection() {
    setupRangerConnectionConfig();
    if (ignoreServerCert) {
      setupRangerIgnoreServerCertificate();
    }
    setupRangerConnectionAuthHeader();
  }

  private void setupRangerConnectionConfig() {
    connectionTimeout = (int) conf.getTimeDuration(
        OZONE_RANGER_OM_CONNECTION_TIMEOUT,
        conf.get(
            OZONE_RANGER_OM_CONNECTION_TIMEOUT,
            OZONE_RANGER_OM_CONNECTION_TIMEOUT_DEFAULT),
        TimeUnit.MILLISECONDS);
    connectionRequestTimeout = (int)conf.getTimeDuration(
        OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT,
        conf.get(
            OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT,
            OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT),
        TimeUnit.MILLISECONDS
    );
    ignoreServerCert = (boolean) conf.getBoolean(
        OZONE_RANGER_OM_IGNORE_SERVER_CERT,
            OZONE_RANGER_OM_IGNORE_SERVER_CERT_DEFAULT);
  }

  private void setupRangerIgnoreServerCertificate() {
    // Create a trust manager that does not validate certificate chains
    TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
          }
          public void checkClientTrusted(
              java.security.cert.X509Certificate[] certs, String authType) {
          }
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

  private void setupRangerConnectionAuthHeader() {
    String userName = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER);
    String passwd = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD);
    String auth = userName + ":" + passwd;
    byte[] encodedAuth =
        Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
    authHeaderValue = "Basic " +
        new String(encodedAuth, StandardCharsets.UTF_8);
  }


  @Override
  public void shutdown() throws Exception {
    // TBD
  }

  @Override
  public void grantAccess(BucketNameSpace bucketNameSpace,
                          BasicUserPrincipal user, ACLType aclType) {
    // TBD
  }

  @Override
  public void revokeAccess(BucketNameSpace bucketNameSpace,
                           BasicUserPrincipal user, ACLType aclType) {
    // TBD
  }

  @Override
  public void grantAccess(AccountNameSpace accountNameSpace,
                          BasicUserPrincipal user, ACLType aclType) {
    // TBD
  }

  @Override
  public void revokeAccess(AccountNameSpace accountNameSpace,
                           BasicUserPrincipal user, ACLType aclType) {
    // TBD
  }

  public List<Pair<BucketNameSpace, ACLType>>
      getAllBucketNameSpaceAccesses(BasicUserPrincipal user) {
    // TBD
    return null;
  }

  @Override
  public boolean checkAccess(BucketNameSpace bucketNameSpace,
                             BasicUserPrincipal user) {
    // TBD
    return true;
  }

  @Override
  public boolean checkAccess(AccountNameSpace accountNameSpace,
                             BasicUserPrincipal user) {
    // TBD
    return true;
  }

  @Override
  public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context)
      throws OMException {
    // TBD
    return true;
  }

  @Override
  public String getRole(OzoneTenantRolePrincipal principal) throws IOException {

    String endpointUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_ROLE_HTTP_ENDPOINT +
            principal.getName();

    HttpsURLConnection conn = makeHttpsGetCall(endpointUrl, "GET", false);
    return getResponseData(conn);
  }

  @Override
  public String getUserId(BasicUserPrincipal principal) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT +
        principal.getName();

    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl,
        "GET", false);
    String response = getResponseData(conn);
    String userIDCreated = null;
    try {
      JsonObject jResonse = new JsonParser().parse(response).getAsJsonObject();
      JsonArray userinfo = jResonse.get("vXUsers").getAsJsonArray();
      int numIndex = userinfo.size();
      for (int i = 0; i < numIndex; ++i) {
        if (userinfo.get(i).getAsJsonObject().get("name").getAsString()
            .equals(principal.getName())) {
          userIDCreated =
              userinfo.get(i).getAsJsonObject().get("id").getAsString();
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

  /**
   * Update the exising role details and push the changes to Ranger.
   *
   * @param principal contains user name, must be an existing user in Ranger.
   * @param existingRole An existing role's JSON response String from Ranger.
   * @param isAdmin Make it delegated admin of the role.
   * @return roleId (not useful for now)
   * @throws IOException
   */
  public String assignUser(BasicUserPrincipal principal, String existingRole,
      boolean isAdmin) throws IOException {

    JsonObject roleObj = new JsonParser().parse(existingRole).getAsJsonObject();
    // Parse Json
    final String roleId = roleObj.get("id").getAsString();
    LOG.debug("Got roleId: {}", roleId);

    JsonArray usersArray = roleObj.getAsJsonArray("users");
    JsonObject newUserEntry = new JsonObject();
    newUserEntry.addProperty("name", principal.getName());
    newUserEntry.addProperty("isAdmin", isAdmin);
    usersArray.add(newUserEntry);
    // Update Json array
    roleObj.add("users", usersArray);

    LOG.debug("Updated: {}", roleObj);

    final String endpointUrl = rangerHttpsAddress +
        OZONE_OM_RANGER_ADMIN_ROLE_ADD_USER_HTTP_ENDPOINT + roleId;
    final String jsonData = roleObj.toString();

    HttpsURLConnection conn =
        makeHttpCall(endpointUrl, jsonData, "PUT", false);
    if (conn.getResponseCode() != HTTP_OK) {
      throw new IOException("Ranger REST API failure: " + conn.getResponseCode()
          + " " + conn.getResponseMessage()
          + ". Error updating Ranger role.");
    }
    String resp = getResponseData(conn);
    String returnedRoleId;
    try {
      JsonObject jObject = new JsonParser().parse(resp).getAsJsonObject();
      returnedRoleId = jObject.get("id").getAsString();
      LOG.debug("Ranger returns roleId: {}", roleId);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return returnedRoleId;
  }

  private String getCreateRoleJsonStr(String roleName, String adminRoleName) {
    return "{"
        + "  \"name\":\"" + roleName + "\","
        + "  \"description\":\"Role created by Ozone for Multi-Tenancy\""
        + (adminRoleName == null ? "" : ", \"roles\":"
        + "[{\"name\":\"" + adminRoleName + "\",\"isAdmin\": true}]")
        + "}";
  }

  public String createRole(OzoneTenantRolePrincipal role, String adminRoleName)
      throws IOException {

    String endpointUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_ROLE_HTTP_ENDPOINT;

    String jsonData = getCreateRoleJsonStr(role.toString(), adminRoleName);

    final HttpsURLConnection conn = makeHttpCall(endpointUrl,
        jsonData, "POST", false);
    if (conn.getResponseCode() != HTTP_OK) {
      throw new IOException("Ranger REST API failure: " + conn.getResponseCode()
          + " " + conn.getResponseMessage()
          + ". Role name '" + role + "' likely already exists in Ranger");
    }
    String roleInfo = getResponseData(conn);
    String roleId;
    try {
      JsonObject jObject = new JsonParser().parse(roleInfo).getAsJsonObject();
      roleId = jObject.get("id").getAsString();
      LOG.debug("Ranger returned roleId: {}", roleId);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return roleId;
  }

  public String createAccessPolicy(AccessPolicy policy) throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT;

    HttpsURLConnection conn = makeHttpCall(rangerAdminUrl,
        policy.serializePolicyToJsonString(),
        "POST", false);
    String policyInfo = getResponseData(conn);
    String policyID;
    try {
      JsonObject jObject = new JsonParser().parse(policyInfo).getAsJsonObject();
      policyID = jObject.get("id").getAsString();
      LOG.debug("policyID is: {}", policyID);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return policyID;
  }

  public AccessPolicy getAccessPolicyByName(String policyName)
      throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_POLICY_HTTP_ENDPOINT +
        policyName;

    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl,
        "GET", false);
    String policyInfo = getResponseData(conn);
    JsonArray jArry = new JsonParser().parse(policyInfo).getAsJsonArray();
    JsonObject jsonObject = jArry.get(0).getAsJsonObject();
    AccessPolicy policy = new RangerAccessPolicy(policyName);
    policy.deserializePolicyFromJsonString(jsonObject);
    return policy;
  }

  public void deleteUser(String userId) throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT
            + userId + "?forceDelete=true";

    HttpsURLConnection conn = makeHttpCall(rangerAdminUrl, null,
        "DELETE", false);
    int respnseCode = conn.getResponseCode();
    if (respnseCode != 200 && respnseCode != 204) {
      throw new IOException("Couldnt delete user " + userId);
    }
  }

  public void deleteRole(String groupId) throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_GROUP_HTTP_ENDPOINT
            + groupId + "?forceDelete=true";

    HttpsURLConnection conn = makeHttpCall(rangerAdminUrl, null,
        "DELETE", false);
    int respnseCode = conn.getResponseCode();
    if (respnseCode != 200 && respnseCode != 204) {
      throw new IOException("Couldnt delete group " + groupId);
    }
  }

  @Override
  public void deletePolicybyName(String policyName) throws Exception {
    AccessPolicy policy = getAccessPolicyByName(policyName);
    String  policyID = policy.getPolicyID();
    LOG.debug("policyID is: {}", policyID);
    deletePolicybyId(policyID);
  }

  public void deletePolicybyId(String policyId) throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT
            + policyId + "?forceDelete=true";
    try {
      HttpsURLConnection conn = makeHttpCall(rangerAdminUrl, null,
          "DELETE", false);
      int respnseCode = conn.getResponseCode();
      if (respnseCode != 200 && respnseCode != 204) {
        throw new IOException("Couldnt delete policy " + policyId);
      }
    } catch (Exception e) {
      throw new IOException("Couldnt delete policy " + policyId, e);
    }
  }

  private String getResponseData(HttpsURLConnection urlConnection)
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
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    return response.toString();
  }

  private HttpsURLConnection makeHttpCall(String urlString,
                                              String jsonInputString,
                                              String method, boolean isSpnego)
      throws IOException {

    URL url = new URL(urlString);
    HttpsURLConnection urlConnection = (HttpsURLConnection)url.openConnection();
    urlConnection.setRequestMethod(method);
    urlConnection.setConnectTimeout(connectionTimeout);
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

  private HttpsURLConnection makeHttpsGetCall(String urlString,
      String method, boolean isSpnego) throws IOException {

    URL url = new URL(urlString);
    HttpsURLConnection urlConnection = (HttpsURLConnection)url.openConnection();
    urlConnection.setRequestMethod(method);
    urlConnection.setConnectTimeout(connectionTimeout);
    urlConnection.setReadTimeout(connectionRequestTimeout);
    urlConnection.setRequestProperty("Accept", "application/json");
    urlConnection.setRequestProperty("Authorization", authHeaderValue);

    return urlConnection;
  }
}
