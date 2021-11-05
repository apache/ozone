package org.apache.hadoop.ozone.om.multitenant;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.http.auth.BasicUserPrincipal;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.om.OMConfigKeys.*;

/**
 * Access controller for multi-tenancy implemented using Ranger's REST API.
 * This class is for testing and is not intended for production use.
 */
public class RangerRestMultiTenantAccessController
    implements MultiTenantAccessController {

  public static final String OZONE_OM_RANGER_ADMIN_POLICY_HTTP_ENDPOINT =
      "/service/public/v2/api/policy/";
  public static final String OZONE_OM_RANGER_ADMIN_ROLE_HTTP_ENDPOINT =
      "/service/public/v2/api/roles/";

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
  private final Map<IAccessAuthorizer.ACLType, String> rangerAclStrings;

  public RangerRestMultiTenantAccessController(Configuration configuration)
      throws IOException {
    conf = new OzoneConfiguration(configuration);
    rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    rangerService = conf.get(OZONE_RANGER_SERVICE);

    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(Role.class, roleSerializer);
    gsonBuilder.registerTypeAdapter(Policy.class, policySerializer);
    gsonBuilder.registerTypeAdapter(BasicUserPrincipal.class, userSerializer);
    jsonConverter = gsonBuilder.create();

    rangerAclStrings = new HashMap<>();
    fillRangerAclStrings();
    initializeRangerConnection();
  }

  private void fillRangerAclStrings() {
    rangerAclStrings.put(IAccessAuthorizer.ACLType.ALL, "All");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.LIST, "List");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ, "Read");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE, "Write");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.CREATE, "Create");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.DELETE, "Delete");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ_ACL, "Read_ACL");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE_ACL, "Write_ACL");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.NONE, "");
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
  public long createPolicy(Policy policy) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_POLICY_HTTP_ENDPOINT;

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        jsonConverter.toJsonTree(policy).getAsJsonObject());
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to create policy %s. " +
          "Http response code: %d", policy.getName(), conn.getResponseCode()));
    }
    String policyInfo = getResponseData(conn);
    long policyID;
    JsonObject jObject = new JsonParser().parse(policyInfo).getAsJsonObject();
    policyID = jObject.get("id").getAsLong();
    return policyID;
  }

  @Override
  public void deletePolicy(long policyID) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_POLICY_HTTP_ENDPOINT
            + policyID;
    HttpsURLConnection conn = makeHttpsDeleteCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to delete policy %d. " +
          "Http response code: %d", policyID, conn.getResponseCode()));
    }
  }

  @Override
  public long createRole(Role role) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_ROLE_HTTP_ENDPOINT;

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        jsonConverter.toJsonTree(role).getAsJsonObject());
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to create role %s. " +
          "Http response code: %d", role.getName(), conn.getResponseCode()));
    }
    String responseString = getResponseData(conn);
    JsonObject jObject = new JsonParser().parse(responseString)
        .getAsJsonObject();
    return jObject.get("id").getAsLong();
  }

  @Override
  public void addUsersToRole(long roleID,
      BasicUserPrincipal... newUsers) throws IOException {
    // Get current role from Ranger.
    JsonObject roleJson = getRoleJson(roleID);
    // Add users to role.
    JsonArray jsonUsers = roleJson.getAsJsonArray("users");
    for (BasicUserPrincipal user: newUsers) {
      jsonUsers.add(jsonConverter.toJsonTree(user));
    }
    // Put modified role back in Ranger.
    putRoleJson(roleID, roleJson);
  }

  @Override
  public void removeUsersFromRole(long roleID,
      BasicUserPrincipal... usersToRemove) throws IOException {
    Set<String> usersToRemoveSet = Stream.of(usersToRemove)
        .map(BasicUserPrincipal::getName)
        .collect(Collectors.toSet());
    // Get current role from Ranger.
    JsonObject roleJson = getRoleJson(roleID);
    // Remove users from role.
    // Json array does not support removal, so we must make a new one.
    JsonArray currentUsers = roleJson.getAsJsonArray("users");
    JsonArray newUsers = new JsonArray();
    for (JsonElement user: currentUsers) {
      String userName = user.getAsJsonObject().get("name").getAsString();
      if (!usersToRemoveSet.contains(userName)) {
        newUsers.add(user);
      }
    }
    roleJson.add("users", newUsers);
    // Put modified role back in Ranger.
    putRoleJson(roleID, roleJson);
  }

  @Override
  public void deleteRole(long roleID) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_POLICY_HTTP_ENDPOINT
            + roleID;
    HttpsURLConnection conn = makeHttpsDeleteCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to delete role %d. " +
          "Http response code: %d", roleID, conn.getResponseCode()));
    }
  }

  @Override
  public void enablePolicy(long policyID) throws Exception {
    setPolicyEnabled(policyID, true);
  }

  @Override
  public void disablePolicy(long policyID) throws Exception {
    setPolicyEnabled(policyID, false);
  }

  private void setPolicyEnabled(long policyID, boolean isEnabled)
      throws IOException {
    // Get current policy from Ranger.
    JsonObject policyJson = getPolicyJson(policyID);
    // Disable the policy.
    policyJson.remove("isEnabled");
    policyJson.addProperty("isEnabled", isEnabled);
    // Put modified policy back in Ranger.
    putPolicyJson(policyID, policyJson);
  }

  private JsonObject getRoleJson(long roleID) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_ROLE_HTTP_ENDPOINT + roleID;

    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to get role %d. " +
          "Http response code: %d", roleID, conn.getResponseCode()));
    }
    String roleInfo = getResponseData(conn);
    return new JsonParser().parse(roleInfo).getAsJsonObject();
  }

  private void putRoleJson(long roleID, JsonObject roleJson)
      throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_ROLE_HTTP_ENDPOINT + roleID;

    HttpsURLConnection conn = makeHttpsPutCall(rangerAdminUrl, roleJson);
    if (!successfulResponseCode(conn.getResponseCode())){
      throw new IOException(String.format("Failed to update role %d. " +
          "Http response code: %d", roleID, conn.getResponseCode()));
    }
  }

  private JsonObject getPolicyJson(long policyID) throws IOException {
    String rangerAdminUrl = rangerHttpsAddress +
        OZONE_OM_RANGER_ADMIN_POLICY_HTTP_ENDPOINT + policyID;

    HttpsURLConnection conn = makeHttpsGetCall(rangerAdminUrl);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to get policy %d. " +
          "Http response code: %d", policyID, conn.getResponseCode()));
    }
    String policyInfo = getResponseData(conn);
    return new JsonParser().parse(policyInfo).getAsJsonObject();
  }

  private void putPolicyJson(long policyID, JsonObject policyJson)
      throws IOException {
    String rangerAdminUrl = rangerHttpsAddress +
        OZONE_OM_RANGER_ADMIN_POLICY_HTTP_ENDPOINT + policyID;

    HttpsURLConnection conn = makeHttpsPutCall(rangerAdminUrl, policyJson);
    if (!successfulResponseCode(conn.getResponseCode())) {
      throw new IOException(String.format("Failed to update policy %d. " +
          "Http response code: %d", policyID, conn.getResponseCode()));
    }
  }

  private HttpsURLConnection makeHttpsPutCall(String url, JsonObject content)
      throws IOException {
    HttpsURLConnection connection = makeBaseHttpsURLConnection(url);
    connection.setRequestMethod("PUT");
    return addJsonContentToConnection(connection, content);
  }

  private HttpsURLConnection makeHttpsPostCall(String url, JsonObject content)
      throws IOException {
    HttpsURLConnection connection = makeBaseHttpsURLConnection(url);
    connection.setRequestMethod("POST");
    return addJsonContentToConnection(connection, content);
  }

  private HttpsURLConnection addJsonContentToConnection(
      HttpsURLConnection connection, JsonObject content) throws IOException {
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

  private final JsonSerializer<Policy> policySerializer =
      new JsonSerializer<Policy>() {
    @Override
    public JsonElement serialize(Policy javaPolicy, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonPolicy = new JsonObject();
      jsonPolicy.addProperty("name", javaPolicy.getName());
      jsonPolicy.addProperty("service", rangerService);
      if (javaPolicy.getDescription().isPresent()) {
        jsonPolicy.addProperty("description",
            javaPolicy.getDescription().get());
      }

      // All resources under this policy are added to this object.
      JsonObject jsonResources = new JsonObject();

      // Add volumes.
      JsonObject jsonVolumeResource = new JsonObject();
      JsonArray jsonVolumeNameArray = new JsonArray();
      for (String volumeName: javaPolicy.getVolumes()) {
        jsonVolumeNameArray.add(new JsonPrimitive(volumeName));
      }
      jsonVolumeResource.add("values", jsonVolumeNameArray);
      jsonVolumeResource.addProperty("isRecursive", false);
      jsonVolumeResource.addProperty("isExcludes", false);
      jsonResources.add("volume", jsonVolumeResource);

      // Add buckets.
      JsonObject jsonBucketResource = new JsonObject();
      JsonArray jsonBucketNameArray = new JsonArray();
      for (String bucketName: javaPolicy.getBuckets()) {
        jsonBucketNameArray.add(new JsonPrimitive(bucketName));
      }
      jsonBucketResource.add("values", jsonBucketNameArray);
      jsonBucketResource.addProperty("isRecursive", false);
      jsonBucketResource.addProperty("isExcludes", false);
      jsonResources.add("bucket", jsonBucketResource);

      // Add keys.
      JsonObject jsonKeyResource = new JsonObject();
      JsonArray jsonKeyNameArray = new JsonArray();
      for (String keyName: javaPolicy.getKeys()) {
        jsonKeyNameArray.add(new JsonPrimitive(keyName));
      }
      jsonKeyResource.add("values", jsonKeyNameArray);
      jsonKeyResource.addProperty("isRecursive", false);
      jsonKeyResource.addProperty("isExcludes", false);
      jsonResources.add("key", jsonKeyResource);

      jsonPolicy.add("resources", jsonResources);

      // Add roles and their acls to the policy.
      JsonArray jsonPolicyItemArray = new JsonArray();

      // Make a new policy item for each role in the map.
      Map<String, Collection<Acl>> roleAcls = javaPolicy.getRoleAcls();
      for (Map.Entry<String, Collection<Acl>> entry: roleAcls.entrySet()) {
        // Add role to the policy item.
        String roleName = entry.getKey();
        JsonObject jsonPolicyItem = new JsonObject();
        JsonArray jsonRoles = new JsonArray();
        jsonRoles.add(new JsonPrimitive(roleName));
        jsonPolicyItem.add("roles", jsonRoles);

        // Add acls to the policy item.
        JsonArray jsonAclArray = new JsonArray();
        for (Acl acl: entry.getValue()) {
          JsonObject jsonAcl  = new JsonObject();
          jsonAcl.addProperty("type",
              rangerAclStrings.get(acl.getAclType()));
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
    @Override
    public JsonElement serialize(Role javaRole, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonRole = new JsonObject();
      jsonRole.addProperty("name", javaRole.getName());

      JsonArray jsonUserArray = new JsonArray();
      for (BasicUserPrincipal javaUser: javaRole.getUsers()) {
        jsonUserArray.add(jsonConverter.toJsonTree(javaUser));
      }

      jsonRole.add("users", jsonUserArray);
      return jsonRole;
    }
  };

  private final JsonSerializer<BasicUserPrincipal> userSerializer =
      new JsonSerializer<BasicUserPrincipal>() {
    @Override
    public JsonElement serialize(BasicUserPrincipal user, Type typeOfSrc,
                                 JsonSerializationContext context) {
        JsonObject jsonMember = new JsonObject();
        jsonMember.addProperty("name", user.getName());
        jsonMember.addProperty("isAdmin", false);
        return jsonMember;
    }
  };
}
