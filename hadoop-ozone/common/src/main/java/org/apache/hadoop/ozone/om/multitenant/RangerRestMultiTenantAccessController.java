package org.apache.hadoop.ozone.om.multitenant;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
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
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_OM_IGNORE_SERVER_CERT_DEFAULT;

public class RangerRestMultiTenantAccessController
    implements MultiTenantAccessController {

  private static final Logger LOG = LoggerFactory
      .getLogger(MultiTenantAccessAuthorizerRangerPlugin.class);

  private OzoneConfiguration conf;
  private boolean ignoreServerCert = false;
  private int connectionTimeout;
  private int connectionRequestTimeout;
  private String authHeaderValue;
  private String rangerHttpsAddress;
  private Gson jsonConverter;

  public RangerRestMultiTenantAccessController(Configuration configuration)
      throws IOException {
    conf = new OzoneConfiguration(configuration);
    rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);

    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(Role.class, roleSerializer);
    gsonBuilder.registerTypeAdapter(Policy.class, policySerializer);
    jsonConverter = gsonBuilder.create();

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
  public long createPolicy(Policy policy) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_CREATE_POLICY_HTTP_ENDPOINT;

    HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl,
        jsonConverter.toJson(policy),
        "POST", false);
    String policyInfo = getResponseData(conn);
    long policyID;
    try {
      JsonObject jObject = new JsonParser().parse(policyInfo).getAsJsonObject();
      policyID = jObject.get("id").getAsLong();
    } catch (JsonParseException e) {
      throw e;
    }
    return policyID;
  }

  @Override
  public void deletePolicy(long policyID) throws IOException {
    String rangerAdminUrl =
        rangerHttpsAddress + OZONE_OM_RANGER_ADMIN_DELETE_POLICY_HTTP_ENDPOINT
            + policyID + "?forceDelete=true";
    try {
      HttpsURLConnection conn = makeHttpsPostCall(rangerAdminUrl, null,
          "DELETE", false);
      int respnseCode = conn.getResponseCode();
      if (respnseCode != 200 && respnseCode != 204) {
        throw new IOException("Couldn't delete policy " + policyID);
      }
    } catch (Exception e) {
      throw new IOException("Couldn't delete policy " + policyID, e);
    }
  }

  @Override
  public long createRole(Role role) throws Exception {
    return 0;
  }

  @Override
  public void addUsersToRole(long roleID, Collection<BasicUserPrincipal> newUsers) throws Exception {
    // Get role, modify user list gson object, put it back.
  }

  @Override
  public void removeUsersFromRole(long roleID, Collection<BasicUserPrincipal> users) throws Exception {
    // Get role, modify user list gson object, put it back.
  }

  @Override
  public void deleteRole(long roleID) throws Exception {

  }

  @Override
  public void enablePolicy(long policyID) throws Exception {
    // Get policy, modify enable gson object, put it back.
  }

  @Override
  public void disablePolicy(long policyID) throws Exception {
    // Get policy, modify enable gson object, put it back.
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

  /// SERIALIZATION ///

  JsonSerializer<Policy> policySerializer = new JsonSerializer<Policy>() {
    @Override
    public JsonElement serialize(Policy javaPolicy, Type typeOfSrc,
                                 JsonSerializationContext context) {
      JsonObject jsonPolicy = new JsonObject();
      jsonPolicy.addProperty("name", javaPolicy.getName());
      // TODO get this from somewhere
      jsonPolicy.addProperty("service","cm_ozone" );

      // All resources under this policy are added to this object.
      JsonObject jsonResources = new JsonObject();

      // Add volume.
      JsonObject jsonVolumeResource = new JsonObject();
      JsonArray jsonVolumeName = new JsonArray();
      jsonVolumeName.add(new JsonPrimitive(javaPolicy.getVolume()));
      jsonVolumeResource.add("values", jsonVolumeName);
      jsonVolumeResource.addProperty("isRecursive", false);
      jsonVolumeResource.addProperty("isExcludes", false);
      jsonResources.add("volume", jsonVolumeResource);

      // Add bucket if it exists.
      if (javaPolicy.getBucket().isPresent()) {
        JsonObject jsonBucketResource = new JsonObject();
        JsonArray jsonBucketName = new JsonArray();
        jsonBucketName.add(new JsonPrimitive(javaPolicy.getBucket().get()));
        jsonBucketResource.add("values", jsonBucketName);
        jsonBucketResource.addProperty("isRecursive", false);
        jsonBucketResource.addProperty("isExcludes", false);
        jsonResources.add("bucket", jsonBucketResource);
      }

      // Add key if it exists.
      if (javaPolicy.getKey().isPresent()) {
        JsonObject jsonKeyResource = new JsonObject();
        JsonArray jsonKeyName = new JsonArray();
        jsonKeyName.add(new JsonPrimitive(javaPolicy.getKey().get()));
        jsonKeyResource.add("values", jsonKeyName);
        jsonKeyResource.addProperty("isRecursive", false);
        jsonKeyResource.addProperty("isExcludes", false);
        jsonResources.add("key", jsonKeyResource);
      }

      jsonPolicy.add("resources", jsonResources);

      if (javaPolicy.getDescription().isPresent()) {
        jsonPolicy.addProperty("description", javaPolicy.getDescription().get());
      }

      // Add roles to the policy.
      JsonArray jsonPolicyItemArray = new JsonArray();
      JsonObject jsonPolicyItem = new JsonObject();
      JsonArray jsonRoles = new JsonArray();
      for (String role: javaPolicy.getRoles()) {
        jsonRoles.add(new JsonPrimitive(role));
      }
      jsonPolicyItem.add("roles", jsonRoles);
      jsonPolicyItemArray.add(jsonPolicyItem);
      jsonPolicy.add("policyItems", jsonPolicyItemArray);

      return jsonPolicy;
    }
  };

  JsonSerializer<Role> roleSerializer = new JsonSerializer<Role>() {
    @Override
    public JsonElement serialize(Role javaRole, Type typeOfSrc,
                                 JsonSerializationContext context) {
      JsonObject jsonRole = new JsonObject();
      jsonRole.addProperty("name", javaRole.getName());

      JsonArray jsonUserArray = new JsonArray();
      for (BasicUserPrincipal javaUser: javaRole.getUsers()) {
        JsonObject jsonRoleMember = new JsonObject();
        jsonRoleMember.addProperty("name", javaUser.getName());
        jsonRoleMember.addProperty("isAdmin", false);

        jsonUserArray.add(jsonRoleMember);
      }

      jsonRole.add("users", jsonUserArray);
      return jsonRole;
    }
  };
}
