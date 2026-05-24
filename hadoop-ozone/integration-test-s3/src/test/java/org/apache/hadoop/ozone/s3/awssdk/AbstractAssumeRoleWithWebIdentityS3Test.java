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

package org.apache.hadoop.ozone.s3.awssdk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.security.SecurityConfig.OZONE_TEST_AUTHORIZATION_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.security.oidc.OidcConfig.OZONE_STS_WEB_IDENTITY_AUDIENCE;
import static org.apache.hadoop.ozone.security.oidc.OidcConfig.OZONE_STS_WEB_IDENTITY_ISSUER_URI;
import static org.apache.hadoop.ozone.security.oidc.OidcConfig.OZONE_STS_WEB_IDENTITY_JWKS_URI;
import static org.apache.hadoop.ozone.security.oidc.OidcConfig.OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.KeyGenerator;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.hadoop.ozone.security.acl.AssumeRoleWithWebIdentityRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.w3c.dom.Document;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Shared mini-cluster harness for WebIdentity STS bootstrap tests.
 */
abstract class AbstractAssumeRoleWithWebIdentityS3Test
    extends ClusterForTests<MiniOzoneCluster> {

  protected static final String AUDIENCE = "ozone";
  protected static final String ALLOWED_BUCKET = "tomato-files";
  protected static final String DENIED_BUCKET = "denied-files";
  protected static final String ROLE_ARN =
      "arn:aws:iam::123456789012:role/tomato-role";
  protected static final String SESSION_NAME = "tomato-session";
  protected static final String PROVIDER_ID = "keycloak";

  private S3GatewayService s3GatewayService;

  protected abstract String issuerUri();

  protected abstract String jwksUri();

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = super.createOzoneConfig();
    conf.setBoolean(OZONE_TEST_AUTHORIZATION_ENABLED, true);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS,
        WebIdentityTestAuthorizer.class.getName());
    conf.setBoolean(OZONE_S3G_STS_HTTP_ENABLED_KEY, true);
    conf.setBoolean(OZONE_STS_WEB_IDENTITY_ENABLED, true);
    conf.set(OZONE_STS_WEB_IDENTITY_ISSUER_URI, issuerUri());
    conf.set(OZONE_STS_WEB_IDENTITY_AUDIENCE, AUDIENCE);
    conf.set(OZONE_STS_WEB_IDENTITY_JWKS_URI, jwksUri());
    conf.setBoolean(OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS, false);
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY, "RATIS");
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_KEY, "ONE");
    conf.set(OZONE_S3G_STS_HTTP_ADDRESS_KEY, localhostWithFreePort());
    conf.set(OZONE_S3G_STS_HTTPS_ADDRESS_KEY, localhostWithFreePort());
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    OzoneManager.setTestSecureOmFlag(true);
    WebIdentityTestAuthorizer.reset();
    s3GatewayService = new S3GatewayService();
    return newClusterBuilder()
        .setNumDatanodes(1)
        .setSecretKeyClient(new InMemorySecretKeyClient())
        .addService(s3GatewayService)
        .build();
  }

  @Override
  protected void onClusterReady() throws Exception {
    try (OzoneClient client =
             Objects.requireNonNull(getCluster().newClient())) {
      client.getObjectStore().createS3Bucket(ALLOWED_BUCKET);
      client.getObjectStore().createS3Bucket(DENIED_BUCKET);
    }
  }

  @AfterAll
  void resetTestSecurityFlag() {
    OzoneManager.setTestSecureOmFlag(false);
  }

  protected StsCredentials assumeRoleWithWebIdentity(String token,
      String expectedSubject) throws Exception {
    HttpResponse response = postSts(token);
    assertEquals(200, response.getCode(), response.getBody());
    Document document = parseXml(response.getBody());
    StsCredentials credentials = new StsCredentials(
        xmlText(document, "AccessKeyId"),
        xmlText(document, "SecretAccessKey"),
        xmlText(document, "SessionToken"));
    assertThat(credentials.getAccessKeyId()).startsWith("ASIA");
    assertThat(credentials.getSecretAccessKey()).isNotBlank();
    assertThat(credentials.getSessionToken()).isNotBlank();
    if (expectedSubject != null) {
      assertThat(xmlText(document, "SubjectFromWebIdentityToken"))
          .isEqualTo(expectedSubject);
    }
    assertThat(xmlText(document, "Audience")).isEqualTo(AUDIENCE);
    assertThat(xmlText(document, "Provider")).isEqualTo(PROVIDER_ID);
    assertThat(xmlText(document, "AssumedRoleId")).contains(SESSION_NAME);
    return credentials;
  }

  protected void assertTemporaryCredentialsAuthorizeS3Operations(
      StsCredentials credentials) {
    try (S3Client s3 = Objects.requireNonNull(s3Client(credentials))) {
      String key = "allowed.txt";
      s3.putObject(PutObjectRequest.builder()
              .bucket(ALLOWED_BUCKET)
              .key(key)
              .build(),
          RequestBody.fromBytes("web identity data".getBytes(UTF_8)));

      ResponseBytes<GetObjectResponse> object =
          s3.getObjectAsBytes(b -> b.bucket(ALLOWED_BUCKET).key(key));
      assertEquals("web identity data", object.asUtf8String());

      assertThat(s3.listObjectsV2(b -> b.bucket(ALLOWED_BUCKET)).contents())
          .anyMatch(item -> key.equals(item.key()));

      S3Exception denied = assertThrows(S3Exception.class, () ->
          s3.putObject(PutObjectRequest.builder()
                  .bucket(DENIED_BUCKET)
                  .key("denied.txt")
                  .build(),
              RequestBody.fromBytes("denied".getBytes(UTF_8))));
      assertThat(denied.statusCode()).isEqualTo(403);
    }
  }

  protected void assertNormalS3RequestWithoutSigV4IsDenied()
      throws Exception {
    URL url = URI.create("http://"
        + s3GatewayService.getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY)
        + "/" + ALLOWED_BUCKET).toURL();
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertThat(connection.getResponseCode()).isEqualTo(403);
  }

  protected void assertStsFails(String token, int expectedStatus)
      throws Exception {
    HttpResponse response = postSts(token);
    assertEquals(expectedStatus, response.getCode(), response.getBody());
    if (token != null) {
      assertThat(response.getBody()).doesNotContain(token);
    }
  }

  protected HttpResponse postSts(String token) throws IOException {
    URL url = URI.create("http://"
        + s3GatewayService.getConf().get(OZONE_S3G_STS_HTTP_ADDRESS_KEY)
        + "/sts").toURL();
    String body = "Action=AssumeRoleWithWebIdentity"
        + "&Version=2011-06-15"
        + "&RoleArn=" + encode(ROLE_ARN)
        + "&RoleSessionName=" + encode(SESSION_NAME)
        + "&ProviderId=" + encode(PROVIDER_ID)
        + "&DurationSeconds=900";
    if (token != null) {
      body += "&WebIdentityToken=" + encode(token);
    }

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type",
        "application/x-www-form-urlencoded");
    try (OutputStream output = connection.getOutputStream()) {
      output.write(body.getBytes(UTF_8));
    }

    int code = connection.getResponseCode();
    String responseBody;
    if (code >= 400) {
      responseBody = IOUtils.toString(connection.getErrorStream(), UTF_8);
    } else {
      responseBody = IOUtils.toString(connection.getInputStream(), UTF_8);
    }
    return new HttpResponse(code, responseBody);
  }

  protected S3Client s3Client(StsCredentials credentials) {
    return s3Client(credentials.getAccessKeyId(), credentials.getSecretAccessKey(),
        credentials.getSessionToken());
  }

  protected S3Client s3Client(String accessKeyId, String secretAccessKey,
      String sessionToken) {
    StaticCredentialsProvider credentialsProvider;
    if (sessionToken == null) {
      credentialsProvider = StaticCredentialsProvider.create(
          AwsBasicCredentials.create(accessKeyId, secretAccessKey));
    } else {
      credentialsProvider = StaticCredentialsProvider.create(
          AwsSessionCredentials.create(accessKeyId, secretAccessKey,
              sessionToken));
    }
    return S3Client.builder()
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create("http://"
            + s3GatewayService.getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY)))
        .credentialsProvider(credentialsProvider)
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .forcePathStyle(true)
        .build();
  }

  protected AssumeRoleWithWebIdentityRequest lastAssumeRoleRequest() {
    return WebIdentityTestAuthorizer.lastAssumeRoleRequest;
  }

  protected int accessChecksWithSessionPolicy() {
    return WebIdentityTestAuthorizer.ACCESS_CHECKS_WITH_SESSION_POLICY.get();
  }

  private static String encode(String value) throws IOException {
    return URLEncoder.encode(value, UTF_8.name());
  }

  private static Document parseXml(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    return factory.newDocumentBuilder()
        .parse(IOUtils.toInputStream(xml, UTF_8));
  }

  private static String xmlText(Document document, String localName) {
    return document.getElementsByTagNameNS("*", localName)
        .item(0).getTextContent();
  }

  protected static final class HttpResponse {
    private final int code;
    private final String body;

    private HttpResponse(int code, String body) {
      this.code = code;
      this.body = body;
    }

    int getCode() {
      return code;
    }

    String getBody() {
      return body;
    }
  }

  protected static final class StsCredentials {
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    private StsCredentials(String accessKeyId, String secretAccessKey,
        String sessionToken) {
      this.accessKeyId = accessKeyId;
      this.secretAccessKey = secretAccessKey;
      this.sessionToken = sessionToken;
    }

    String getAccessKeyId() {
      return accessKeyId;
    }

    String getSecretAccessKey() {
      return secretAccessKey;
    }

    String getSessionToken() {
      return sessionToken;
    }
  }

  public static final class WebIdentityTestAuthorizer
      implements IAccessAuthorizer {
    private static final AtomicInteger ACCESS_CHECKS_WITH_SESSION_POLICY =
        new AtomicInteger();
    private static volatile AssumeRoleWithWebIdentityRequest
        lastAssumeRoleRequest;

    public static void reset() {
      ACCESS_CHECKS_WITH_SESSION_POLICY.set(0);
      lastAssumeRoleRequest = null;
    }

    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      if (context.getSessionPolicy() == null) {
        return true;
      }
      ACCESS_CHECKS_WITH_SESSION_POLICY.incrementAndGet();
      if (!(ozoneObject instanceof OzoneObj)) {
        return false;
      }
      OzoneObj obj = (OzoneObj) ozoneObject;
      return obj.getBucketName() == null
          || ALLOWED_BUCKET.equals(obj.getBucketName());
    }

    @Override
    public String generateAssumeRoleWithWebIdentitySessionPolicy(
        AssumeRoleWithWebIdentityRequest request) throws OMException {
      recordAssumeRoleRequest(request);
      if (!"tomato-user".equals(request.getUser())
          || !request.getGroups().contains("ozone-tomato")
          || !ROLE_ARN.equals(request.getRoleArn())
          || !PROVIDER_ID.equals(request.getProviderId())
          || StringUtils.isBlank(request.getSubject())) {
        throw new OMException("WebIdentity role assumption denied",
            OMException.ResultCodes.ACCESS_DENIED);
      }
      return "{"
          + "\"Version\":\"2012-10-17\","
          + "\"Statement\":[{"
          + "\"Effect\":\"Allow\","
          + "\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:ListBucket\"],"
          + "\"Resource\":[\"arn:aws:s3:::" + ALLOWED_BUCKET
          + "\",\"arn:aws:s3:::" + ALLOWED_BUCKET + "/*\"]"
          + "}]}";
    }

    private static void recordAssumeRoleRequest(
        AssumeRoleWithWebIdentityRequest request) {
      lastAssumeRoleRequest = request;
    }
  }

  private static final class InMemorySecretKeyClient
      implements SecretKeyClient {
    private final Map<UUID, ManagedSecretKey> keys = new LinkedHashMap<>();
    private final ManagedSecretKey current;

    private InMemorySecretKeyClient() {
      this.current = newKey();
      keys.put(current.getId(), current);
    }

    @Override
    public ManagedSecretKey getCurrentSecretKey() {
      return current;
    }

    @Override
    public ManagedSecretKey getSecretKey(UUID id) {
      return keys.get(id);
    }

    private static ManagedSecretKey newKey() {
      try {
        KeyGenerator generator = KeyGenerator.getInstance("HmacSHA256");
        return new ManagedSecretKey(UUID.randomUUID(), Instant.now(),
            Instant.now().plus(Duration.ofHours(4)), generator.generateKey());
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("HmacSHA256 is unavailable", e);
      }
    }
  }
}
