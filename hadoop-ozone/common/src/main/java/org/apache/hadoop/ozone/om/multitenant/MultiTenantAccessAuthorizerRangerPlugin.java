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
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_ROLE_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_POLICY_ID_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_ROLE_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_ROLE_ADD_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ALL_POLICIES_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_DOWNLOAD_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_OZONE_SERVICE_ENDPOINT;
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
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
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
import org.apache.kerby.util.Base64;
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

  private MultiTenantAccessController accessController;

  private OzoneConfiguration conf;
  private boolean ignoreServerCert = true;
  private int connectionTimeout;
  private int connectionRequestTimeout;
  private String authHeaderValue;
  private String rangerHttpsAddress;

  @Override
  public void init(Configuration configuration) throws IOException {
    conf = new OzoneConfiguration(configuration);
    accessController = new RangerRestMultiTenantAccessController(conf);
    rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    if (rangerHttpsAddress == null) {
      throw new OMException("Config ozone.om.ranger.https-address is not set! "
          + "Multi-Tenancy feature requires Apache Ranger to function properly",
          OMException.ResultCodes.INTERNAL_ERROR);
    }
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
  public void shutdown() throws IOException {
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

    HttpURLConnection conn = makeHttpGetCall(endpointUrl, "GET", false);
    return getResponseData(conn);
  }

  @Override
  public String getRole(String roleName) throws IOException {

    String endpointUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_ROLE_HTTP_ENDPOINT +
            roleName;

    HttpURLConnection conn = makeHttpGetCall(endpointUrl, "GET", false);
    return getResponseData(conn);
  }

  @Override
  public String getUserId(BasicUserPrincipal principal) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT +
        principal.getName();

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl,
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
   * @return roleId (not useful for now)
   * @throws IOException
   */
  @Override
  public String revokeUserFromRole(BasicUserPrincipal principal,
      String existingRole) throws IOException {
    JsonObject roleObj = new JsonParser().parse(existingRole).getAsJsonObject();
    // Parse Json
    final String roleId = roleObj.get("id").getAsString();
    LOG.debug("Got roleId: {}", roleId);

    JsonArray oldUsersArray = roleObj.getAsJsonArray("users");
    JsonArray newUsersArray = new JsonArray();

    for (int i = 0; i < oldUsersArray.size(); ++i) {
      JsonObject newUserEntry = oldUsersArray.get(i).getAsJsonObject();
      if (!newUserEntry.get("name").getAsString().equals(principal.getName())) {
        newUsersArray.add(newUserEntry);
      }
      // Update Json array
    }
    roleObj.add("users", newUsersArray);

    LOG.debug("Updated: {}", roleObj);

    final String endpointUrl = rangerHttpsAddress +
        OZONE_OM_RANGER_ADMIN_ROLE_ADD_USER_HTTP_ENDPOINT + roleId;
    final String jsonData = roleObj.toString();

    HttpURLConnection conn =
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

  /**
   * Update the exising role details and push the changes to Ranger.
   *
   * @param principal contains user name, must be an existing user in Ranger.
   * @param existingRole An existing role's JSON response String from Ranger.
   * @param isAdmin Make it delegated admin of the role.
   * @return roleId (not useful for now)
   * @throws IOException
   */
  public String assignUserToRole(BasicUserPrincipal principal,
      String existingRole, boolean isAdmin) throws IOException {

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

    HttpURLConnection conn =
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

  /**
   * Update the exising role details and push the changes to Ranger.
   *
   * @param users must be existing users in Ranger.
   * @param existingRole An existing role's JSON response String from Ranger.
   * @return roleId (not useful for now)
   * @throws IOException
   */
  @Override
  public String assignAllUsers(HashSet<String> users,
                               String existingRole) throws IOException {

    JsonObject roleObj = new JsonParser().parse(existingRole).getAsJsonObject();
    // Parse Json
    final String roleId = roleObj.get("id").getAsString();
    LOG.debug("Got roleId: {}", roleId);

    JsonArray usersArray = new JsonArray();
    for (String user: users) {
      JsonObject newUserEntry = new JsonObject();
      newUserEntry.addProperty("name", user);
      newUserEntry.addProperty("isAdmin", false);
      usersArray.add(newUserEntry);
    }
    // Update Json array
    roleObj.remove("users"); // remove the old users
    roleObj.add("users", usersArray);

    LOG.debug("Updated: {}", roleObj);

    final String endpointUrl = rangerHttpsAddress +
        OZONE_OM_RANGER_ADMIN_ROLE_ADD_USER_HTTP_ENDPOINT + roleId;
    final String jsonData = roleObj.toString();

    HttpURLConnection conn =
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

  public String createRole(String role, String adminRoleName)
      throws IOException {

    String endpointUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_ROLE_HTTP_ENDPOINT;

    String jsonData = getCreateRoleJsonStr(role, adminRoleName);

    final HttpURLConnection conn = makeHttpCall(endpointUrl,
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

  private String getCreateUserJsonStr(String userName, String password) {
    return "{"
        + "  \"name\":\"" +  userName + "\","
        + "  \"password\":\"" +  password + "\","
        + "  \"firstName\":\"" +  userName + "\","
        + "  \"userRoleList\":[\"ROLE_USER\"]"
        + "}";
  }

  public String createUser(String userName, String password)
      throws IOException {

    String endpointUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_USER_HTTP_ENDPOINT;

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
      JsonObject jObject = new JsonParser().parse(userInfo).getAsJsonObject();
      userId = jObject.get("id").getAsString();
      LOG.debug("Ranger returned userId: {}", userId);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return userId;
  }


  public String createAccessPolicy(AccessPolicy policy) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT;

    HttpURLConnection conn = makeHttpCall(rangerAdminUrl,
        policy.serializePolicyToJsonString(),
        "POST", false);
    String policyInfo = getResponseData(conn);
    String policyID;
    try {
      JsonObject jObject = new JsonParser().parse(policyInfo).getAsJsonObject();
      // TODO: Use policy name instead of id
      policyID = jObject.get("id").getAsString();
      LOG.debug("policyID is: {}", policyID);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return policyID;
  }

  public AccessPolicy getAccessPolicyByName(String policyName)
      throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_POLICY_HTTP_ENDPOINT +
        policyName;

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl,
        "GET", false);
    String policyInfo = getResponseData(conn);
    JsonArray jArry = new JsonParser().parse(policyInfo).getAsJsonArray();
    JsonObject jsonObject = jArry.get(0).getAsJsonObject();
    AccessPolicy policy = new RangerAccessPolicy(policyName);
    policy.deserializePolicyFromJsonString(jsonObject);
    return policy;
  }

  @Override
  public AccessPolicy getAccessPolicyById(String policyId)
      throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_POLICY_ID_HTTP_ENDPOINT +
            policyId;

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl,
        "GET", false);
    String policyInfo = getResponseData(conn);
    JsonArray jArry = new JsonParser().parse(policyInfo).getAsJsonArray();
    JsonObject jsonObject = jArry.get(0).getAsJsonObject();
    AccessPolicy policy =
        new RangerAccessPolicy(jsonObject.get("name").getAsString());
    policy.deserializePolicyFromJsonString(jsonObject);
    return policy;
  }

  /**
   * Return the service ID for Ozone service in Ranger.
   * TODO: Error handling when Ozone service doesn't exist in Ranger.
   */
  public int getRangerOzoneServiceId() throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_OZONE_SERVICE_ENDPOINT;
    int id = 0;

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl,
        "GET", false);
    String sInfo = getResponseData(conn);
    JsonObject jObject = new JsonParser().parse(sInfo).getAsJsonObject();
    JsonArray jArry = jObject.getAsJsonArray("services");
    for (int i = 0; i < jArry.size(); ++i) {
      JsonObject serviceObj = jArry.get(i).getAsJsonObject();
      String serviceName = serviceObj.get("type").getAsString();
      if (!serviceName.equals("ozone")) {
        continue;
      }
      id = serviceObj.get("id").getAsInt();
    }
    return id;
  }

  public long getCurrentOzoneServiceVersion(int ozoneServiceId)
      throws IOException {
    String rangerAdminUrl = rangerHttpsAddress
        + OZONE_OM_RANGER_OZONE_SERVICE_ENDPOINT + ozoneServiceId;

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl, "GET", false);
    String sInfo = getResponseData(conn);
    JsonObject jObject = new JsonParser().parse(sInfo).getAsJsonObject();
    return jObject.get("policyVersion").getAsLong();
  }

  public String getIncrementalRangerChanges(long baseVersion)
      throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_DOWNLOAD_ENDPOINT + baseVersion;

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl, "GET", false);
    String sInfo = getResponseData(conn);
    return sInfo;
  }

  public String getAllMultiTenantPolicies(int ozoneServiceId)
      throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ALL_POLICIES_ENDPOINT
            + ozoneServiceId + "?policyLabelsPartial=OzoneMultiTenant";

    HttpURLConnection conn = makeHttpGetCall(rangerAdminUrl,
        "GET", false);
    String sInfo = getResponseData(conn);
    return sInfo;
  }

  @Override
  public MultiTenantAccessController getMultiTenantAccessController() {
    return this.accessController;
  }

  public void deleteUser(String userId) throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT
            + userId + "?forceDelete=true";

    HttpURLConnection conn = makeHttpCall(rangerAdminUrl, null,
        "DELETE", false);
    int respnseCode = conn.getResponseCode();
    if (respnseCode != 200 && respnseCode != 204) {
      throw new IOException("Couldn't delete user " + userId);
    }
  }

  public void deleteRole(String roleName) throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_ROLE_HTTP_ENDPOINT
            + roleName + "?forceDelete=true";

    HttpURLConnection conn = makeHttpCall(rangerAdminUrl, null,
        "DELETE", false);
    int respnseCode = conn.getResponseCode();
    if (respnseCode != 200 && respnseCode != 204) {
      throw new IOException("Couldn't delete role " + roleName);
    }
  }

  @Override
  public void deletePolicybyName(String policyName) throws IOException {
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
      HttpURLConnection conn = makeHttpCall(rangerAdminUrl, null,
          "DELETE", false);
      int respnseCode = conn.getResponseCode();
      if (respnseCode != 200 && respnseCode != 204) {
        throw new IOException("Couldnt delete policy " + policyId);
      }
    } catch (Exception e) {
      throw new IOException("Couldnt delete policy " + policyId, e);
    }
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
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    return response.toString();
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

  /**
   * Can make either http or https request.
   */
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
}
