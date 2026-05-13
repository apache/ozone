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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ALLOW_INSECURE_HTTP_FOR_TESTS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_AUDIENCE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ISSUER_URI;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_URI;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_HTTPS_ADDRESS_KEY;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.KeyGenerator;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.io.IOUtils;
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
import org.junit.jupiter.api.Test;
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
 * End-to-end coverage for the WebIdentity STS bootstrap path and the existing
 * S3 SigV4 temporary credential validation path.
 */
class TestAssumeRoleWithWebIdentityEndToEnd
    extends ClusterForTests<MiniOzoneCluster> {

  private static final String ISSUER = "http://keycloak.test/realms/ozone";
  private static final String AUDIENCE = "ozone";
  private static final String ALLOWED_BUCKET = "tomato-files";
  private static final String DENIED_BUCKET = "denied-files";
  private static final String ROLE_ARN =
      "arn:aws:iam::123456789012:role/tomato-role";
  private static final String SESSION_NAME = "tomato-session";

  private final TestJwtIssuer jwtIssuer = new TestJwtIssuer();
  private S3GatewayService s3GatewayService;

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = super.createOzoneConfig();
    conf.setBoolean(OZONE_TEST_AUTHORIZATION_ENABLED, true);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS,
        WebIdentityEndToEndAuthorizer.class.getName());
    conf.setBoolean(OZONE_S3G_STS_HTTP_ENABLED_KEY, true);
    conf.setBoolean(OZONE_STS_WEB_IDENTITY_ENABLED, true);
    conf.set(OZONE_STS_WEB_IDENTITY_ISSUER_URI, ISSUER);
    conf.set(OZONE_STS_WEB_IDENTITY_AUDIENCE, AUDIENCE);
    conf.set(OZONE_STS_WEB_IDENTITY_JWKS_URI, jwtIssuer.jwksUri());
    conf.setBoolean(OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS, false);
    conf.setBoolean(OZONE_STS_WEB_IDENTITY_ALLOW_INSECURE_HTTP_FOR_TESTS,
        true);
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY, "RATIS");
    conf.set(OZONE_SERVER_DEFAULT_REPLICATION_KEY, "ONE");
    conf.set(OZONE_S3G_STS_HTTP_ADDRESS_KEY, localhostWithFreePort());
    conf.set(OZONE_S3G_STS_HTTPS_ADDRESS_KEY, localhostWithFreePort());
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    OzoneManager.setTestSecureOmFlag(true);
    WebIdentityEndToEndAuthorizer.reset();
    s3GatewayService = new S3GatewayService();
    return newClusterBuilder()
        .setNumDatanodes(1)
        .setSecretKeyClient(new InMemorySecretKeyClient())
        .addService(s3GatewayService)
        .build();
  }

  @Override
  protected void onClusterReady() throws Exception {
    try (OzoneClient client = getCluster().newClient()) {
      client.getObjectStore().createS3Bucket(ALLOWED_BUCKET);
      client.getObjectStore().createS3Bucket(DENIED_BUCKET);
    }
  }

  @AfterAll
  void resetTestSecurityFlag() {
    OzoneManager.setTestSecureOmFlag(false);
  }

  @Test
  void webIdentityTemporaryCredentialsAuthorizeS3Operations()
      throws Exception {
    StsCredentials credentials = assumeRoleWithWebIdentity(
        jwtIssuer.token("tomato-user", "subject-tomato",
            Collections.singletonList("ozone-tomato"), AUDIENCE,
            Instant.now().plus(Duration.ofHours(1))));

    AssumeRoleWithWebIdentityRequest request =
        WebIdentityEndToEndAuthorizer.lastAssumeRoleRequest;
    assertThat(request).isNotNull();
    assertEquals("tomato-user", request.getUser());
    assertThat(request.getGroups()).containsExactly("ozone-tomato");
    assertEquals(ROLE_ARN, request.getRoleArn());
    assertEquals(SESSION_NAME, request.getRoleSessionName());
    assertEquals(ISSUER, request.getIssuer());
    assertEquals(AUDIENCE, request.getAudience());

    try (S3Client s3 = s3Client(credentials)) {
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

      assertThrows(S3Exception.class, () ->
          s3.putObject(PutObjectRequest.builder()
                  .bucket(DENIED_BUCKET)
                  .key("denied.txt")
                  .build(),
              RequestBody.fromBytes("denied".getBytes(UTF_8))));
    }

    assertThat(WebIdentityEndToEndAuthorizer.accessChecksWithSessionPolicy)
        .hasPositiveValue();
  }

  @Test
  void invalidWebIdentityTokensFailBeforeCredentialsAreIssued()
      throws Exception {
    HttpResponse missingToken = postSts(null);
    assertThat(missingToken.code).isGreaterThanOrEqualTo(400);

    assertStsFails(jwtIssuer.token("tomato-user", "subject-tomato",
        Collections.singletonList("ozone-tomato"), "wrong-audience",
        Instant.now().plus(Duration.ofHours(1))), 403);

    assertStsFails(jwtIssuer.token("tomato-user", "subject-tomato",
        Collections.singletonList("ozone-tomato"), AUDIENCE,
        Instant.now().minus(Duration.ofMinutes(5))), 403);

    assertStsFails(jwtIssuer.algNoneToken(), 403);

    assertStsFails(jwtIssuer.tamperedGroupsToken(), 403);

    assertStsFails(jwtIssuer.token("denied-user", "subject-denied",
        Collections.singletonList("ozone-denied"), AUDIENCE,
        Instant.now().plus(Duration.ofHours(1))), 403);
  }

  @Test
  void normalS3RequestWithoutSigV4IsStillDenied() throws Exception {
    URL url = URI.create("http://"
        + s3GatewayService.getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY)
        + "/" + ALLOWED_BUCKET).toURL();
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertThat(connection.getResponseCode()).isEqualTo(403);
  }

  @Test
  void temporaryCredentialsRequireCorrectSecretAndSessionToken()
      throws Exception {
    StsCredentials credentials = assumeRoleWithWebIdentity(
        jwtIssuer.token("tomato-user", "subject-tomato",
            Collections.singletonList("ozone-tomato"), AUDIENCE,
            Instant.now().plus(Duration.ofHours(1))));

    try (S3Client missingSessionToken =
             s3Client(credentials.accessKeyId, credentials.secretAccessKey,
                 null)) {
      assertThrows(S3Exception.class, () ->
          missingSessionToken.listObjectsV2(b -> b.bucket(ALLOWED_BUCKET)));
    }

    try (S3Client wrongSessionToken =
             s3Client(credentials.accessKeyId, credentials.secretAccessKey,
                 credentials.sessionToken + "wrong")) {
      assertThrows(S3Exception.class, () ->
          wrongSessionToken.listObjectsV2(b -> b.bucket(ALLOWED_BUCKET)));
    }

    try (S3Client wrongSecret =
             s3Client(credentials.accessKeyId,
                 credentials.secretAccessKey + "wrong",
                 credentials.sessionToken)) {
      assertThrows(S3Exception.class, () ->
          wrongSecret.listObjectsV2(b -> b.bucket(ALLOWED_BUCKET)));
    }
  }

  private StsCredentials assumeRoleWithWebIdentity(String token)
      throws Exception {
    HttpResponse response = postSts(token);
    assertEquals(200, response.code, response.body);
    Document document = parseXml(response.body);
    StsCredentials credentials = new StsCredentials(
        xmlText(document, "AccessKeyId"),
        xmlText(document, "SecretAccessKey"),
        xmlText(document, "SessionToken"));
    assertThat(credentials.accessKeyId).startsWith("ASIA");
    assertThat(credentials.secretAccessKey).isNotBlank();
    assertThat(credentials.sessionToken).isNotBlank();
    assertThat(xmlText(document, "SubjectFromWebIdentityToken"))
        .isEqualTo("subject-tomato");
    assertThat(xmlText(document, "Audience")).isEqualTo(AUDIENCE);
    assertThat(xmlText(document, "AssumedRoleId")).contains(SESSION_NAME);
    return credentials;
  }

  private void assertStsFails(String token, int expectedStatus)
      throws Exception {
    HttpResponse response = postSts(token);
    assertEquals(expectedStatus, response.code, response.body);
    assertThat(response.body).doesNotContain(token);
  }

  private HttpResponse postSts(String token) throws IOException {
    URL url = URI.create("http://"
        + s3GatewayService.getConf().get(OZONE_S3G_STS_HTTP_ADDRESS_KEY)
        + "/sts").toURL();
    String body = "Action=AssumeRoleWithWebIdentity"
        + "&Version=2011-06-15"
        + "&RoleArn=" + encode(ROLE_ARN)
        + "&RoleSessionName=" + encode(SESSION_NAME)
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

  private S3Client s3Client(StsCredentials credentials) {
    return s3Client(credentials.accessKeyId, credentials.secretAccessKey,
        credentials.sessionToken);
  }

  private S3Client s3Client(String accessKeyId, String secretAccessKey,
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

  private static final class HttpResponse {
    private final int code;
    private final String body;

    private HttpResponse(int code, String body) {
      this.code = code;
      this.body = body;
    }
  }

  private static final class StsCredentials {
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    private StsCredentials(String accessKeyId, String secretAccessKey,
        String sessionToken) {
      this.accessKeyId = accessKeyId;
      this.secretAccessKey = secretAccessKey;
      this.sessionToken = sessionToken;
    }
  }

  public static final class WebIdentityEndToEndAuthorizer
      implements IAccessAuthorizer {
    private static final AtomicInteger accessChecksWithSessionPolicy =
        new AtomicInteger();
    private static volatile AssumeRoleWithWebIdentityRequest
        lastAssumeRoleRequest;

    public static void reset() {
      accessChecksWithSessionPolicy.set(0);
      lastAssumeRoleRequest = null;
    }

    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      if (context.getSessionPolicy() == null) {
        return true;
      }
      accessChecksWithSessionPolicy.incrementAndGet();
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
      lastAssumeRoleRequest = request;
      if (!"tomato-user".equals(request.getUser())
          || !request.getGroups().contains("ozone-tomato")
          || !ROLE_ARN.equals(request.getRoleArn())) {
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

  private static final class TestJwtIssuer {
    private final RSAKey key;
    private final Path jwksFile;

    private TestJwtIssuer() {
      try {
        this.key = rsaKey("kid-primary");
        this.jwksFile = Files.createTempFile(
            "ozone-webidentity-jwks", ".json");
        JWKSet jwkSet = new JWKSet(Collections.singletonList(
            key.toPublicJWK()));
        Files.write(jwksFile, jwkSet.toString().getBytes(UTF_8));
      } catch (Exception e) {
        throw new IllegalStateException("Failed to create test JWKS", e);
      }
    }

    private String jwksUri() {
      return jwksFile.toUri().toString();
    }

    private String token(String username, String subject,
        Iterable<String> groups, String audience, Instant expiresAt)
        throws Exception {
      return signedToken(key, key.getKeyID(), username, subject, groups,
          audience, expiresAt);
    }

    private String tamperedGroupsToken() throws Exception {
      String token = token("tomato-user", "subject-tomato",
          Collections.singletonList("ozone-tomato"), AUDIENCE,
          Instant.now().plus(Duration.ofHours(1)));
      String[] parts = token.split("\\.");
      String payload = new String(java.util.Base64.getUrlDecoder()
          .decode(parts[1]), UTF_8);
      parts[1] = java.util.Base64.getUrlEncoder().withoutPadding()
          .encodeToString(payload.replace("ozone-tomato", "ozone-admins")
              .getBytes(UTF_8));
      return String.join(".", parts);
    }

    private String algNoneToken() {
      return new PlainJWT(claims("tomato-user", "subject-tomato",
          Collections.singletonList("ozone-tomato"), AUDIENCE,
          Instant.now().plus(Duration.ofHours(1))).build()).serialize();
    }

    private static String signedToken(RSAKey signerKey, String keyId,
        String username, String subject, Iterable<String> groups,
        String audience, Instant expiresAt) throws Exception {
      SignedJWT jwt = new SignedJWT(
          new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(keyId).build(),
          claims(username, subject, groups, audience, expiresAt).build());
      jwt.sign(new RSASSASigner(signerKey.toRSAPrivateKey()));
      return jwt.serialize();
    }

    private static JWTClaimsSet.Builder claims(String username, String subject,
        Iterable<String> groups, String audience, Instant expiresAt) {
      Map<String, Object> realmAccess = new LinkedHashMap<>();
      realmAccess.put("roles", Arrays.asList("writer", "read"));
      Instant now = Instant.now();
      return new JWTClaimsSet.Builder()
          .issuer(ISSUER)
          .subject(subject)
          .audience(Collections.singletonList(audience))
          .issueTime(Date.from(now.minusSeconds(30)))
          .expirationTime(Date.from(expiresAt))
          .claim("preferred_username", username)
          .claim("groups", groups)
          .claim("realm_access", realmAccess);
    }

    private static RSAKey rsaKey(String keyId) throws Exception {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      KeyPair keyPair = generator.generateKeyPair();
      return new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
          .privateKey((RSAPrivateKey) keyPair.getPrivate())
          .keyID(keyId)
          .algorithm(JWSAlgorithm.RS256)
          .build();
    }
  }
}
