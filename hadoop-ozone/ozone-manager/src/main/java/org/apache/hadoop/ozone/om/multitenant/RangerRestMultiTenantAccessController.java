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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.kerby.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;

/**
 * Access controller for multi-tenancy implemented using Ranger's REST API.
 * This class is for testing and is not intended for production use.
 *
 * TODO: REMOVE.
 */
public class RangerRestMultiTenantAccessController
    implements MultiTenantAccessController {

  public static final String OZONE_RANGER_POLICY_HTTP_ENDPOINT =
      "/service/public/v2/api/policy/";

  public static final String OZONE_RANGER_ROLE_HTTP_ENDPOINT =
      "/service/public/v2/api/roles/";

  private String getPolicyByNameEndpoint(String policyName) {
    // /service/public/v2/api/service/{servicename}/policy/{policyname}
    return rangerHttpsAddress + "/service/public/v2/api/service/" +
        rangerService + "/policy/" + policyName;
  }

  private String getRoleByNameEndpoint(String roleName) {
    // /service/public/v2/api/roles/name/
    return rangerHttpsAddress + "/service/public/v2/api/roles/name/" + roleName;
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(RangerRestMultiTenantAccessController.class);

  private final OzoneConfiguration conf;
  private boolean ignoreServerCert = false;
  private int connectionTimeout;
  private int connectionRequestTimeout;
  private String authHeaderValue;
  private final String rangerHttpsAddress;
  private final Gson jsonConverter;
  private final String rangerService;
  private final Map<IAccessAuthorizer.ACLType, String> aclToString;
  private final Map<String, IAccessAuthorizer.ACLType> stringToAcl;
  private long lastPolicyUpdateTimeEpochMillis = -1;

  public RangerRestMultiTenantAccessController(Configuration configuration)
      throws IOException {
    conf = new OzoneConfiguration(configuration);
    rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    rangerService = conf.get(OZONE_RANGER_SERVICE);

    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(Policy.class, policySerializer);
    gsonBuilder.registerTypeAdapter(Policy.class, policyDeserializer);
    gsonBuilder.registerTypeAdapter(Role.class, roleSerializer);
    gsonBuilder.registerTypeAdapter(Role.class, roleDeserializer);
    gsonBuilder.registerTypeAdapter(BasicUserPrincipal.class, userSerializer);
    jsonConverter = gsonBuilder.create();

    aclToString = new EnumMap<>(IAccessAuthorizer.ACLType.class);
    stringToAcl = new HashMap<>();
    fillRangerAclStrings();
    initializeRangerConnection();
  }

  private void fillRangerAclStrings() {
    aclToString.put(IAccessAuthorizer.ACLType.ALL, "all");
    aclToString.put(IAccessAuthorizer.ACLType.LIST, "list");
    aclToString.put(IAccessAuthorizer.ACLType.READ, "read");
    aclToString.put(IAccessAuthorizer.ACLType.WRITE, "write");
    aclToString.put(IAccessAuthorizer.ACLType.CREATE, "create");
    aclToString.put(IAccessAuthorizer.ACLType.DELETE, "delete");
    aclToString.put(IAccessAuthorizer.ACLType.READ_ACL, "read_acl");
    aclToString.put(IAccessAuthorizer.ACLType.WRITE_ACL, "write_acl");
    aclToString.put(IAccessAuthorizer.ACLType.NONE, "");

    stringToAcl.put("all", IAccessAuthorizer.ACLType.ALL);
    stringToAcl.put("list", IAccessAuthorizer.ACLType.LIST);
    stringToAcl.put("read", IAccessAuthorizer.ACLType.READ);
    stringToAcl.put("write", IAccessAuthorizer.ACLType.WRITE);
    stringToAcl.put("create", IAccessAuthorizer.ACLType.CREATE);
    stringToAcl.put("delete", IAccessAuthorizer.ACLType.DELETE);
    stringToAcl.put("read_acl", IAccessAuthorizer.ACLType.READ_ACL);
    stringToAcl.put("write_acl", IAccessAuthorizer.ACLType.WRITE_ACL);
    stringToAcl.put("", IAccessAuthorizer.ACLType.NONE);
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
    ignoreServerCert = conf.getBoolean(
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
  public Policy createPolicy(Policy policy) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_RANGER_POLICY_HTTP_ENDPOINT;
    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        jsonConverter.toJsonTree(policy).getAsJsonObject());
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to create policy %s. " +
          "Http response code: %d", policy.getName(), conn.getResponseCode()));
    }
    getResponseData(conn);

    // TODO: Should reconstruct from response data.
    return policy;
  }

  @Override
  public void deletePolicy(String policyName) throws IOException {
    String rangerAdminUrl = getPolicyByNameEndpoint(policyName);
    HttpsURLConnection conn = makeHttpsDeleteCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to delete policy '%s'. " +
          "Http response code: %d", policyName, conn.getResponseCode()));
    }
  }

  public Map<Long, Policy> getPolicies() throws Exception {
    // This API gets all policies for all services. The
    // /public/v2/api/policies/{serviceDefName}/for-resource endpoint is
    // supposed to get policies for only a specified service, but it does not
    // seem to work. This implementation should be ok for testing purposes as
    // this class is intended.
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_RANGER_POLICY_HTTP_ENDPOINT;
    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to get all policies. " +
          "Http response code: %d", conn.getResponseCode()));
    }
    String allPoliciesString = getResponseData(conn);
    // Filter out policies not for Ozone service.
    JsonArray jsonPoliciesArray = new JsonParser().parse(allPoliciesString)
        .getAsJsonArray();
    Map<Long, Policy> policies = new HashMap<>();
    for (JsonElement jsonPolicy: jsonPoliciesArray) {
      JsonObject jsonPolicyObject = jsonPolicy.getAsJsonObject();
      String service = jsonPolicyObject.get("service").getAsString();
      if (service.equals(rangerService)) {
        long id = jsonPolicyObject.get("id").getAsLong();
        policies.put(id, jsonConverter.fromJson(jsonPolicyObject,
            Policy.class));
      }
    }

    return policies;
  }

  @Override
  public Policy getPolicy(String policyName) throws IOException {
    String rangerAdminUrl = getPolicyByNameEndpoint(policyName);

    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to get policy '%s'. " +
          "Http response code: %d", policyName, conn.getResponseCode()));
    }
    String policyInfo = getResponseData(conn);
    return jsonConverter.fromJson(policyInfo, Policy.class);
  }

  @Override
  public List<Policy> getLabeledPolicies(String label) throws IOException {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public Policy updatePolicy(Policy policy) throws IOException {
    throw new NotImplementedException("Not Implemented");
  }

  public void updatePolicy(long policyID, Policy policy) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_RANGER_POLICY_HTTP_ENDPOINT + policyID;

    HttpsURLConnection conn = makeHttpsPutCall(rangerAdminUrl,
        jsonConverter.toJsonTree(policy));
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to update policy %d. " +
          "Http response code: %d", policyID, conn.getResponseCode()));
    }
  }

  @Override
  public Role createRole(Role role) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_RANGER_ROLE_HTTP_ENDPOINT;

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        jsonConverter.toJsonTree(role).getAsJsonObject());
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to create role %s. " +
          "Http response code: %d", role.getName(), conn.getResponseCode()));
    }
    String responseString = getResponseData(conn);
    JsonObject jObject = new JsonParser().parse(responseString)
        .getAsJsonObject();
//    return jObject.get("id").getAsLong();

    // TODO: Should reconstruct from response data.
    return role;
  }

  @Override
  public void deleteRole(String roleName) throws IOException {
    String rangerAdminUrl = getRoleByNameEndpoint(roleName);
    HttpsURLConnection conn = makeHttpsDeleteCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to delete role '%s'. " +
          "Http response code: %d", roleName, conn.getResponseCode()));
    }
  }

  @Override
  public long getRangerServicePolicyVersion() throws IOException {
    throw new NotImplementedException("Not Implemented");
  }

  public Map<Long, Role> getRoles() throws Exception {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_RANGER_ROLE_HTTP_ENDPOINT;
    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to get all roles. " +
          "Http response code: %d", conn.getResponseCode()));
    }

    String allRolesString = getResponseData(conn);
    JsonArray rolesArrayJson =
        new JsonParser().parse(allRolesString).getAsJsonArray();
    Map<Long, Role> roles = new HashMap<>();
    for (JsonElement roleJson: rolesArrayJson) {
      long id = roleJson.getAsJsonObject().get("id").getAsLong();
      roles.put(id, jsonConverter.fromJson(roleJson, Role.class));
    }

    return roles;
  }

  @Override
  public Role getRole(String roleName) throws IOException {
    String rangerAdminUrl = getRoleByNameEndpoint(roleName);

    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to get role '%s'. " +
          "Http response code: %d", roleName, conn.getResponseCode()));
    }
    String roleInfo = getResponseData(conn);
    return jsonConverter.fromJson(roleInfo, Role.class);
  }

  @Override
  public Role updateRole(long roleId, Role role) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_RANGER_ROLE_HTTP_ENDPOINT + roleId;

    HttpsURLConnection conn = makeHttpsPutCall(rangerAdminUrl,
        jsonConverter.toJsonTree(role));
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to update role %d. " +
          "Http response code: %d", roleId, conn.getResponseCode()));
    }

    // TODO: Should reconstruct from response data.
    return role;
  }

  private HttpsURLConnection makeHttpsPutCall(String url, JsonElement content)
      throws IOException {
    HttpsURLConnection connection = makeBaseHttpsURLConnection(url);
    connection.setRequestMethod("PUT");
    return addJsonContentToConnection(connection, content);
  }

  private HttpsURLConnection makeHttpsPostCall(String url, JsonElement content)
      throws IOException {
    HttpsURLConnection connection = makeBaseHttpsURLConnection(url);
    connection.setRequestMethod("POST");
    return addJsonContentToConnection(connection, content);
  }

  private HttpsURLConnection addJsonContentToConnection(
      HttpsURLConnection connection, JsonElement content) throws IOException {
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json;");
    try (OutputStream os = connection.getOutputStream()) {
      byte[] input = content.toString().getBytes(StandardCharsets.UTF_8);
      os.write(input, 0, input.length);
      os.flush();
    }

    return connection;
  }

  private HttpsURLConnection makeHttpsGetCall(String urlString)
      throws IOException {
    HttpsURLConnection connection = makeBaseHttpsURLConnection(urlString);
    connection.setRequestMethod("GET");
    return connection;
  }

  private HttpsURLConnection makeHttpsDeleteCall(String urlString)
      throws IOException {
    HttpsURLConnection connection = makeBaseHttpsURLConnection(urlString);
    connection.setRequestMethod("DELETE");
    return connection;
  }

  private HttpsURLConnection makeBaseHttpsURLConnection(String urlString)
      throws IOException {
    URL url = new URL(urlString);
    HttpsURLConnection urlConnection = (HttpsURLConnection)url.openConnection();
    urlConnection.setConnectTimeout(connectionTimeout);
    urlConnection.setReadTimeout(connectionRequestTimeout);
    urlConnection.setRequestProperty("Accept", "application/json");
    urlConnection.setRequestProperty("Authorization", authHeaderValue);

    return urlConnection;
  }

  private String getResponseData(HttpsURLConnection urlConnection)
      throws IOException {
    StringBuilder response = new StringBuilder();
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(
            urlConnection.getInputStream(), StandardCharsets.UTF_8))) {
      String responseLine;
      while ((responseLine = br.readLine()) != null) {
        response.append(responseLine.trim());
      }
    }
    return response.toString();
  }

  private boolean successfulResponseCode(long responseCode) {
    return responseCode >= 200 && responseCode < 300;
  }

  /// SERIALIZATION ///

  private final JsonDeserializer<Policy> policyDeserializer =
      new JsonDeserializer<Policy>() {
        @Override public Policy deserialize(JsonElement jsonElement, Type type,
            JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {
          JsonObject policyJson = jsonElement.getAsJsonObject();
          String name = policyJson.get("name").getAsString();
          Policy.Builder policyB = new Policy.Builder();
          policyB.setName(name);
          if (policyJson.has("description")) {
            policyB.setDescription(policyJson.get("description").getAsString());
          }
          policyB.setEnabled(policyJson.get("isEnabled").getAsBoolean());

          // Read volume, bucket, keys from json.
          JsonObject resourcesJson =
              policyJson.get("resources").getAsJsonObject();
          // All Ozone Ranger policies specify at least a volume.
          JsonObject jsonVolumeResource =
              resourcesJson.get("volume").getAsJsonObject();
          JsonArray volumes = jsonVolumeResource.get("values").getAsJsonArray();
          volumes.forEach(vol -> policyB.addVolume(vol.getAsString()));

          if (resourcesJson.has("bucket")) {
            JsonObject jsonBucketResource =
                resourcesJson.get("bucket").getAsJsonObject();
            JsonArray buckets =
                jsonBucketResource.get("values").getAsJsonArray();
            buckets.forEach(bucket -> policyB.addBucket(bucket.getAsString()));
          }

          if (resourcesJson.has("key")) {
            JsonObject jsonKeysResource =
                resourcesJson.get("key").getAsJsonObject();
            JsonArray keys = jsonKeysResource.get("values").getAsJsonArray();
            keys.forEach(key -> policyB.addKey(key.getAsString()));
          }

          // Read Roles and their ACLs.
          JsonArray policyItemsJson = policyJson.getAsJsonArray("policyItems");
          for (JsonElement policyItemElement : policyItemsJson) {
            JsonObject policyItemJson = policyItemElement.getAsJsonObject();
            JsonArray jsonRoles = policyItemJson.getAsJsonArray("roles");
            JsonArray jsonAclArray = policyItemJson.getAsJsonArray("accesses");

            for (JsonElement jsonAclElem : jsonAclArray) {
              JsonObject jsonAcl = jsonAclElem.getAsJsonObject();
              String aclType = jsonAcl.get("type").getAsString();
              Acl acl;
              if (jsonAcl.get("isAllowed").getAsBoolean()) {
                acl = Acl.allow(stringToAcl.get(aclType));
              } else {
                acl = Acl.deny(stringToAcl.get(aclType));
              }

              for (JsonElement roleNameJson : jsonRoles) {
                policyB.addRoleAcl(roleNameJson.getAsString(),
                    Collections.singleton(acl));
              }
            }
          }

          return policyB.build();
        }
      };

  private final JsonDeserializer<Role> roleDeserializer =
      new JsonDeserializer<Role>() {
        @Override public Role deserialize(JsonElement jsonElement, Type type,
            JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {
          JsonObject roleJson = jsonElement.getAsJsonObject();
          String name = roleJson.get("name").getAsString();
          Role.Builder role = new Role.Builder();
          role.setName(name);
          if (roleJson.has("description")) {
            role.setDescription(roleJson.get("description").getAsString());
          }
          for (JsonElement jsonUser : roleJson.get("users").getAsJsonArray()) {
            String userName =
                jsonUser.getAsJsonObject().get("name").getAsString();
            role.addUser(userName, false);
          }

          return role.build();
        }
      };

  private final JsonSerializer<Policy> policySerializer =
      new JsonSerializer<Policy>() {
        @Override public JsonElement serialize(Policy javaPolicy,
            Type typeOfSrc, JsonSerializationContext context) {
          JsonObject jsonPolicy = new JsonObject();
          jsonPolicy.addProperty("name", javaPolicy.getName());
          jsonPolicy.addProperty("service", rangerService);
          jsonPolicy.addProperty("isEnabled", javaPolicy.isEnabled());
          if (javaPolicy.getDescription().isPresent()) {
            jsonPolicy.addProperty("description",
                javaPolicy.getDescription().get());
          }

          // All resources under this policy are added to this object.
          JsonObject jsonResources = new JsonObject();

          // Add volumes. Ranger requires at least one volume to be specified.
          JsonArray jsonVolumeNameArray = new JsonArray();
          for (String volumeName : javaPolicy.getVolumes()) {
            jsonVolumeNameArray.add(new JsonPrimitive(volumeName));
          }
          JsonObject jsonVolumeResource = new JsonObject();
          jsonVolumeResource.add("values", jsonVolumeNameArray);
          jsonVolumeResource.addProperty("isRecursive", false);
          jsonVolumeResource.addProperty("isExcludes", false);
          jsonResources.add("volume", jsonVolumeResource);

          // Add buckets.
          JsonArray jsonBucketNameArray = new JsonArray();
          for (String bucketName : javaPolicy.getBuckets()) {
            jsonBucketNameArray.add(new JsonPrimitive(bucketName));
          }

          if (jsonBucketNameArray.size() > 0) {
            JsonObject jsonBucketResource = new JsonObject();
            jsonBucketResource.add("values", jsonBucketNameArray);
            jsonBucketResource.addProperty("isRecursive", false);
            jsonBucketResource.addProperty("isExcludes", false);
            jsonResources.add("bucket", jsonBucketResource);
          }

          // Add keys.
          JsonArray jsonKeyNameArray = new JsonArray();
          for (String keyName : javaPolicy.getKeys()) {
            jsonKeyNameArray.add(new JsonPrimitive(keyName));
          }
          if (jsonKeyNameArray.size() > 0) {
            JsonObject jsonKeyResource = new JsonObject();
            jsonKeyResource.add("values", jsonKeyNameArray);
            jsonKeyResource.addProperty("isRecursive", false);
            jsonKeyResource.addProperty("isExcludes", false);
            jsonResources.add("key", jsonKeyResource);
          }

          jsonPolicy.add("resources", jsonResources);

          // Add roles and their acls to the policy.
          JsonArray jsonPolicyItemArray = new JsonArray();

          // Make a new policy item for each role in the map.
          Map<String, Collection<Acl>> roleAcls = javaPolicy.getRoleAcls();
          for (Map.Entry<String, Collection<Acl>> entry : roleAcls.entrySet()) {
            // Add role to the policy item.
            String roleName = entry.getKey();
            JsonObject jsonPolicyItem = new JsonObject();
            JsonArray jsonRoles = new JsonArray();
            jsonRoles.add(new JsonPrimitive(roleName));
            jsonPolicyItem.add("roles", jsonRoles);

            // Add acls to the policy item.
            JsonArray jsonAclArray = new JsonArray();
            for (Acl acl : entry.getValue()) {
              JsonObject jsonAcl = new JsonObject();
              jsonAcl.addProperty("type", aclToString.get(acl.getAclType()));
              jsonAcl.addProperty("isAllowed", acl.isAllowed());
              jsonAclArray.add(jsonAcl);
              jsonPolicyItem.add("accesses", jsonAclArray);
            }
            jsonPolicyItemArray.add(jsonPolicyItem);
          }
          jsonPolicy.add("policyItems", jsonPolicyItemArray);

          return jsonPolicy;
        }
      };

  private final JsonSerializer<Role> roleSerializer =
      new JsonSerializer<Role>() {
        @Override public JsonElement serialize(Role javaRole, Type typeOfSrc,
            JsonSerializationContext context) {
          JsonObject jsonRole = new JsonObject();
          jsonRole.addProperty("name", javaRole.getName());

          JsonArray jsonUserArray = new JsonArray();
          for (String javaUser : javaRole.getUsersMap().keySet()) {
            jsonUserArray.add(jsonConverter.toJsonTree(javaUser));
          }

          jsonRole.add("users", jsonUserArray);
          return jsonRole;
        }
      };

  private final JsonSerializer<BasicUserPrincipal> userSerializer =
      new JsonSerializer<BasicUserPrincipal>() {
        @Override public JsonElement serialize(BasicUserPrincipal user,
            Type typeOfSrc, JsonSerializationContext context) {
          JsonObject jsonMember = new JsonObject();
          jsonMember.addProperty("name", user.getName());
          jsonMember.addProperty("isAdmin", false);
          return jsonMember;
        }
      };

  public void setPolicyLastUpdateTime(long mtime) {
    lastPolicyUpdateTimeEpochMillis = mtime;
  }

  public long getPolicyLastUpdateTime() {
    return lastPolicyUpdateTimeEpochMillis;
  }

  public HashSet<String> getRoleList() {
    return null;
  }
}
