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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_GROUP_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_GROUP_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_USER_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_GROUP_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT;
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
import java.util.stream.Collectors;

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
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements MultiTenantAccessAuthorizer.
 */
public class MultiTenantAccessAuthorizerRangerPlugin implements
    MultiTenantAccessAuthorizer {
  private static final Logger LOG = LoggerFactory
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
                          OzoneMultiTenantPrincipal user, ACLType aclType) {
    // TBD
  }

  @Override
  public void revokeAccess(BucketNameSpace bucketNameSpace,
                           OzoneMultiTenantPrincipal user, ACLType aclType) {
    // TBD
  }

  @Override
  public void grantAccess(AccountNameSpace accountNameSpace,
                          OzoneMultiTenantPrincipal user, ACLType aclType) {
    // TBD
  }

  @Override
  public void revokeAccess(AccountNameSpace accountNameSpace,
                           OzoneMultiTenantPrincipal user, ACLType aclType) {
    // TBD
  }

  public List<Pair<BucketNameSpace, ACLType>>
      getAllBucketNameSpaceAccesses(OzoneMultiTenantPrincipal user) {
    // TBD
    return null;
  }

  @Override
  public boolean checkAccess(BucketNameSpace bucketNameSpace,
                             OzoneMultiTenantPrincipal user) {
    // TBD
    return true;
  }

  @Override
  public boolean checkAccess(AccountNameSpace accountNameSpace,
                             OzoneMultiTenantPrincipal user) {
    // TBD
    return true;
  }

  @Override
  public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context)
      throws OMException {
    // TBD
    return true;
  }
  private String getCreateUserJsonString(String userName,
                                         List<String> groupIDs)
      throws Exception {
    String groupIdList = groupIDs.stream().collect(Collectors.joining("\",\"",
        "", ""));
    String jsonCreateUserString = "{ \"name\":\"" + userName  + "\"," +
        "\"firstName\":\"" + userName + "\"," +
        "  \"loginId\": \"" + userName + "\"," +
        "  \"password\" : \"user1pass\"," +
        "  \"userRoleList\":[\"ROLE_USER\"]," +
        "  \"groupIdList\":[\"" + groupIdList +"\"] " +
        " }";
    return jsonCreateUserString;
  }

  @Override
  public String getGroupId(OzoneMultiTenantPrincipal principal)
      throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_GROUP_HTTP_ENDPOINT +
            principal.getFullMultiTenantPrincipalID();

    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl,
        "GET", false);
    String response = getResponseData(conn);
    String groupIDCreated = null;
    try {
      JsonObject jResonse = new JsonParser().parse(response).getAsJsonObject();
      JsonArray info = jResonse.get("vXGroups").getAsJsonArray();
      int numIndex = info.size();
      for (int i = 0; i < numIndex; ++i) {
        if (info.get(i).getAsJsonObject().get("name").getAsString()
            .equals(principal.getFullMultiTenantPrincipalID())) {
          groupIDCreated =
              info.get(i).getAsJsonObject().get("id").getAsString();
          break;
        }
      }
      System.out.println("Group ID is : " + groupIDCreated);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return groupIDCreated;
  }

  @Override
  public String getUserId(OzoneMultiTenantPrincipal principal)
      throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_GET_USER_HTTP_ENDPOINT +
        principal.getFullMultiTenantPrincipalID();

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
            .equals(principal.getFullMultiTenantPrincipalID())) {
          userIDCreated =
              userinfo.get(i).getAsJsonObject().get("id").getAsString();
          break;
        }
      }
      System.out.println("User ID is : " + userIDCreated);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return userIDCreated;
  }

  public String createUser(OzoneMultiTenantPrincipal principal,
                           List<String> groupIDs)
      throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_USER_HTTP_ENDPOINT;

    String jsonCreateUserString = getCreateUserJsonString(
        principal.getFullMultiTenantPrincipalID(), groupIDs);

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        jsonCreateUserString, "POST", false);
    String userInfo = getResponseData(conn);
    String userIDCreated;
    try {
      JsonObject jObject = new JsonParser().parse(userInfo).getAsJsonObject();
      userIDCreated = jObject.get("id").getAsString();
      System.out.println("User ID is : " + userIDCreated);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return userIDCreated;
  }

  private String getCreateGroupJsonString(String groupName) throws Exception {
    String jsonCreateGroupString = "{ \"name\":\"" + groupName + "\"," +
        "  \"description\":\"test\" " +
        " }";
    return jsonCreateGroupString;
  }


  public String createGroup(OzoneMultiTenantPrincipal group) throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_GROUP_HTTP_ENDPOINT;

    String jsonCreateGroupString = getCreateGroupJsonString(
        group.getFullMultiTenantPrincipalID());

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        jsonCreateGroupString,
        "POST", false);
    String groupInfo = getResponseData(conn);
    String groupIdCreated;
    try {
      JsonObject jObject = new JsonParser().parse(groupInfo).getAsJsonObject();
      groupIdCreated = jObject.get("id").getAsString();
      System.out.println("GroupID is: " + groupIdCreated);
    } catch (JsonParseException e) {
      e.printStackTrace();
      throw e;
    }
    return groupIdCreated;
  }

  public String createAccessPolicy(AccessPolicy policy) throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT;

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        policy.serializePolicyToJsonString(),
        "POST", false);
    String policyInfo = getResponseData(conn);
    String policyID;
    try {
      JsonObject jObject = new JsonParser().parse(policyInfo).getAsJsonObject();
      policyID = jObject.get("id").getAsString();
      System.out.println("policyID is : " + policyID);
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

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl, null,
        "DELETE", false);
    int respnseCode = conn.getResponseCode();
    if (respnseCode != 200 && respnseCode != 204) {
      throw new IOException("Couldnt delete user " + userId);
    }
  }

  public void deleteGroup(String groupId) throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_GROUP_HTTP_ENDPOINT
            + groupId + "?forceDelete=true";

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl, null,
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
    System.out.println("policyID is : " + policyID);
    deletePolicybyId(policyID);
  }

  public void deletePolicybyId(String policyId) throws IOException {

    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT
            + policyId + "?forceDelete=true";
    try {
      HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl, null,
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
        new InputStreamReader(urlConnection.getInputStream(), "utf-8"))) {
      String responseLine;
      while ((responseLine = br.readLine()) != null) {
        response.append(responseLine.trim());
      }
      System.out.println(response);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    return response.toString();
  }

  private HttpsURLConnection makeHttpsPostCall(String urlString,
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

    if ((jsonInputString !=null) && !jsonInputString.isEmpty()) {
      urlConnection.setDoOutput(true);
      urlConnection.setRequestProperty("Content-Type", "application/json;");
      try (OutputStream os = urlConnection.getOutputStream()) {
        byte[] input = jsonInputString.getBytes("utf-8");
        os.write(input, 0, input.length);
        os.flush();
      }
    }

    return urlConnection;
  }

  private HttpsURLConnection makeHttpsGetCall(String urlString,
                                               String method, boolean isSpnego)
      throws IOException, AuthenticationException {

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
