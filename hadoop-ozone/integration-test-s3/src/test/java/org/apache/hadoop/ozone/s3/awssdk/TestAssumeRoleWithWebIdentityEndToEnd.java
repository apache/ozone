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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.ozone.security.acl.AssumeRoleWithWebIdentityRequest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * End-to-end coverage for the WebIdentity STS bootstrap path using a generated
 * JWT and local JWKS file.
 */
class TestAssumeRoleWithWebIdentityEndToEnd
    extends AbstractAssumeRoleWithWebIdentityS3Test {

  private static final String ISSUER = "http://keycloak.test/realms/ozone";

  private final TestJwtIssuer jwtIssuer = new TestJwtIssuer();

  @Override
  protected String issuerUri() {
    return ISSUER;
  }

  @Override
  protected String jwksUri() {
    return jwtIssuer.jwksUri();
  }

  @Test
  void webIdentityTemporaryCredentialsAuthorizeS3Operations()
      throws Exception {
    StsCredentials credentials = assumeRoleWithWebIdentity(
        jwtIssuer.token("tomato-user", "subject-tomato",
            Collections.singletonList("ozone-tomato"), AUDIENCE,
            Instant.now().plus(Duration.ofHours(1))), "subject-tomato");

    AssumeRoleWithWebIdentityRequest request = lastAssumeRoleRequest();
    assertThat(request).isNotNull();
    assertEquals("tomato-user", request.getUser());
    assertThat(request.getGroups()).containsExactly("ozone-tomato");
    assertEquals(ROLE_ARN, request.getRoleArn());
    assertEquals(SESSION_NAME, request.getRoleSessionName());
    assertEquals(ISSUER, request.getIssuer());
    assertEquals("subject-tomato", request.getSubject());
    assertEquals(AUDIENCE, request.getAudience());
    assertEquals(PROVIDER_ID, request.getProviderId());

    assertTemporaryCredentialsAuthorizeS3Operations(credentials);
    assertThat(accessChecksWithSessionPolicy()).isPositive();
  }

  @Test
  void invalidWebIdentityTokensFailBeforeCredentialsAreIssued()
      throws Exception {
    HttpResponse missingToken = postSts(null);
    assertThat(missingToken.getCode()).isGreaterThanOrEqualTo(400);

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
    assertNormalS3RequestWithoutSigV4IsDenied();
  }

  @Test
  void temporaryCredentialsRequireCorrectSecretAndSessionToken()
      throws Exception {
    StsCredentials credentials = assumeRoleWithWebIdentity(
        jwtIssuer.token("tomato-user", "subject-tomato",
            Collections.singletonList("ozone-tomato"), AUDIENCE,
            Instant.now().plus(Duration.ofHours(1))), "subject-tomato");

    try (S3Client missingSessionToken =
             s3Client(credentials.getAccessKeyId(),
                 credentials.getSecretAccessKey(),
                 null)) {
      assertThrows(S3Exception.class, () ->
          missingSessionToken.listObjectsV2(b -> b.bucket(ALLOWED_BUCKET)));
    }

    try (S3Client wrongSessionToken =
             s3Client(credentials.getAccessKeyId(),
                 credentials.getSecretAccessKey(),
                 credentials.getSessionToken() + "wrong")) {
      assertThrows(S3Exception.class, () ->
          wrongSessionToken.listObjectsV2(b -> b.bucket(ALLOWED_BUCKET)));
    }

    try (S3Client wrongSecret =
             s3Client(credentials.getAccessKeyId(),
                 credentials.getSecretAccessKey() + "wrong",
                 credentials.getSessionToken())) {
      assertThrows(S3Exception.class, () ->
          wrongSecret.listObjectsV2(b -> b.bucket(ALLOWED_BUCKET)));
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
