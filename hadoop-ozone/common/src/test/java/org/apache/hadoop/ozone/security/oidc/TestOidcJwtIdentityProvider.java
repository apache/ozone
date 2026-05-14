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

package org.apache.hadoop.ozone.security.oidc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OidcJwtIdentityProvider}.
 */
public class TestOidcJwtIdentityProvider {

  private static final String ISSUER =
      "https://keycloak.example.com/realms/ozone";
  private static final String AUDIENCE = "ozone";
  private static final Instant NOW = Instant.parse("2026-05-13T00:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

  private RSAKey primaryKey;
  private RSAKey rotatedKey;
  private RSAKey wrongKey;

  @BeforeEach
  public void setUp() throws Exception {
    primaryKey = rsaKey("kid-primary");
    rotatedKey = rsaKey("kid-rotated");
    wrongKey = rsaKey("kid-wrong");
  }

  @Test
  public void validJwtPasses() throws Exception {
    String jwt = token(primaryKey);
    OzoneIdentity identity = provider(primaryKey)
        .authenticate(AuthCredentials.bearerToken(jwt));

    assertThat(identity.getUsername()).isEqualTo("tomato-user");
    assertThat(identity.getSubject()).isEqualTo("subject-tomato");
    assertThat(identity.getIssuer()).isEqualTo(ISSUER);
    assertThat(identity.getAuthMethod()).isEqualTo("oidc");
    assertThat(identity.getGroups()).containsExactly("ozone-tomato");
    assertThat(identity.getRoles()).containsExactly("writer", "read");
    assertThat(identity.getExpiresAt()).isEqualTo(NOW.plusSeconds(3600));
    assertThat(identity.toString()).doesNotContain(jwt);
  }

  @Test
  public void expiredJwtFails() throws Exception {
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.expirationTime(Date.from(NOW.minusSeconds(120))));

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("expired");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void notBeforeInFutureFails() throws Exception {
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.notBeforeTime(Date.from(NOW.plusSeconds(120))));

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("not valid yet");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void issueTimeInFutureFails() throws Exception {
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.issueTime(Date.from(NOW.plusSeconds(120))));

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("issue time");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void wrongIssuerFails() throws Exception {
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.issuer("https://other.example.com/realms/ozone"));

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("issuer");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void wrongAudienceFails() throws Exception {
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.audience(Collections.singletonList("other-audience")));

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("audience");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void wrongSignatureFails() throws Exception {
    String jwt = token(wrongKey, primaryKey.getKeyID(), builder -> {
    });

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("signature");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void manipulatedGroupsClaimFailsSignatureValidation()
      throws Exception {
    String jwt = token(primaryKey);
    String manipulatedJwt = replacePayloadText(jwt, "ozone-tomato",
        "ozone-admins");

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey)
            .authenticate(AuthCredentials.bearerToken(manipulatedJwt)));

    assertThat(exception).hasMessageContaining("signature");
    assertThat(exception.toString()).doesNotContain(manipulatedJwt);
  }

  @Test
  public void algNoneFails() {
    String jwt = new PlainJWT(baseClaims().build()).serialize();

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("signed");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void unknownKidTriggersJwksRefresh() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CachingJwksProvider jwksProvider = new CachingJwksProvider(() -> {
      if (fetches.incrementAndGet() == 1) {
        return jwkSet(primaryKey);
      }
      return jwkSet(rotatedKey);
    }, Duration.ofMinutes(10), CLOCK);

    OzoneIdentity identity = provider(config(), jwksProvider)
        .authenticate(AuthCredentials.bearerToken(token(rotatedKey)));

    assertThat(identity.getUsername()).isEqualTo("tomato-user");
    assertThat(fetches).hasValue(2);
  }

  @Test
  public void unknownKidRefreshIsDebounced() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CachingJwksProvider jwksProvider = new CachingJwksProvider(() -> {
      fetches.incrementAndGet();
      return jwkSet(primaryKey);
    }, Duration.ofMinutes(10), Duration.ofSeconds(5), CLOCK);
    OidcJwtIdentityProvider provider = provider(config(), jwksProvider);

    assertThrows(OidcAuthenticationException.class, () ->
        provider.authenticate(AuthCredentials.bearerToken(token(rotatedKey))));
    assertThrows(OidcAuthenticationException.class, () ->
        provider.authenticate(AuthCredentials.bearerToken(token(wrongKey))));

    assertThat(fetches).hasValue(2);
  }

  @Test
  public void keyRotationWorks() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    CachingJwksProvider jwksProvider = new CachingJwksProvider(() -> {
      if (fetches.incrementAndGet() == 1) {
        return jwkSet(primaryKey);
      }
      return jwkSet(primaryKey, rotatedKey);
    }, Duration.ofMinutes(10), CLOCK);
    OidcJwtIdentityProvider provider = provider(config(), jwksProvider);

    provider.authenticate(AuthCredentials.bearerToken(token(primaryKey)));
    provider.authenticate(AuthCredentials.bearerToken(token(rotatedKey)));
    OzoneIdentity identity =
        provider.authenticate(AuthCredentials.bearerToken(token(primaryKey)));

    assertThat(identity.getUsername()).isEqualTo("tomato-user");
    assertThat(fetches).hasValue(2);
  }

  @Test
  public void usernameClaimMappingWorks() throws Exception {
    OidcConfig config = configBuilder()
        .setUsernameClaim("email")
        .build();
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.claim("email", "tomato@example.com"));

    OzoneIdentity identity = provider(config, jwksProvider(primaryKey))
        .authenticate(AuthCredentials.bearerToken(jwt));

    assertThat(identity.getUsername()).isEqualTo("tomato@example.com");
  }

  @Test
  public void nestedGroupsAndRolesClaimMappingWorks() throws Exception {
    OidcConfig config = configBuilder()
        .setGroupsClaim("resource_access.ozone.groups")
        .setRolesClaim("resource_access.ozone.roles")
        .build();
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder -> {
      Map<String, Object> ozone = new LinkedHashMap<>();
      ozone.put("groups", Arrays.asList("ozone-tomato", "ozone-admins"));
      ozone.put("roles", Collections.singletonList("writer"));
      builder.claim("resource_access",
          Collections.singletonMap("ozone", ozone));
    });

    OzoneIdentity identity = provider(config, jwksProvider(primaryKey))
        .authenticate(AuthCredentials.bearerToken(jwt));

    assertThat(identity.getGroups())
        .containsExactly("ozone-tomato", "ozone-admins");
    assertThat(identity.getRoles()).containsExactly("writer");
  }

  @Test
  public void missingUsernameFailsClosed() throws Exception {
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.claim("preferred_username", null));

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception).hasMessageContaining("required claim");
    assertThat(exception.toString()).doesNotContain(jwt);
  }

  @Test
  public void missingGroupsAreMappedToEmptySet() throws Exception {
    String jwt = token(primaryKey, primaryKey.getKeyID(), builder ->
        builder.claim("groups", null));

    OzoneIdentity identity = provider(primaryKey)
        .authenticate(AuthCredentials.bearerToken(jwt));

    assertThat(identity.getGroups()).isEmpty();
  }

  @Test
  public void jwksFetcherEnforcesSizeLimit() throws Exception {
    Path jwksFile = Files.createTempFile("ozone-test-jwks", ".json");
    Files.write(jwksFile,
        jwkSet(primaryKey).toString().getBytes(StandardCharsets.UTF_8));
    UrlJwksFetcher fetcher = new UrlJwksFetcher(jwksFile.toUri().toURL(),
        Duration.ofSeconds(5), Duration.ofSeconds(5), 8);

    assertThrows(IOException.class, fetcher::fetch);
  }

  @Test
  public void tokenMaterialIsNotIncludedInParseException() {
    String jwt = "sensitive-token-material";

    OidcAuthenticationException exception = assertThrows(
        OidcAuthenticationException.class,
        () -> provider(primaryKey).authenticate(AuthCredentials.bearerToken(jwt)));

    assertThat(exception.toString()).doesNotContain(jwt);
  }

  private OidcJwtIdentityProvider provider(RSAKey key) {
    return provider(config(), jwksProvider(key));
  }

  private OidcJwtIdentityProvider provider(OidcConfig config,
      JwksProvider jwksProvider) {
    return new OidcJwtIdentityProvider(config, jwksProvider, CLOCK);
  }

  private static OidcConfig config() {
    return configBuilder().build();
  }

  private static OidcConfig.Builder configBuilder() {
    return OidcConfig.newBuilder()
        .setIssuerUri(ISSUER)
        .setAudience(AUDIENCE)
        .setClockSkew(Duration.ofSeconds(60));
  }

  private static JwksProvider jwksProvider(RSAKey... keys) {
    JWKSet jwkSet = jwkSet(keys);
    return keyId -> {
      if (keyId == null || keyId.trim().isEmpty()) {
        return new ArrayList<>(jwkSet.getKeys());
      }
      JWK key = jwkSet.getKeyByKeyId(keyId);
      return key == null ? Collections.emptyList()
          : Collections.singletonList(key);
    };
  }

  private static JWKSet jwkSet(RSAKey... keys) {
    List<JWK> publicKeys = new ArrayList<>();
    for (RSAKey key : keys) {
      publicKeys.add(key.toPublicJWK());
    }
    return new JWKSet(publicKeys);
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

  private static String token(RSAKey signerKey) throws Exception {
    return token(signerKey, signerKey.getKeyID(), builder -> {
    });
  }

  private static String token(RSAKey signerKey, String keyId,
      ClaimsCustomizer customizer) throws Exception {
    JWTClaimsSet.Builder claims = baseClaims();
    customizer.customize(claims);
    SignedJWT signedJWT = new SignedJWT(
        new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(keyId).build(),
        claims.build());
    signedJWT.sign(new RSASSASigner(signerKey.toRSAPrivateKey()));
    return signedJWT.serialize();
  }

  private static String replacePayloadText(String jwt, String from,
      String to) {
    String[] parts = jwt.split("\\.");
    String payload = new String(Base64.getUrlDecoder().decode(parts[1]),
        StandardCharsets.UTF_8);
    String manipulatedPayload = payload.replace(from, to);
    parts[1] = Base64.getUrlEncoder().withoutPadding().encodeToString(
        manipulatedPayload.getBytes(StandardCharsets.UTF_8));
    return String.join(".", parts);
  }

  private static JWTClaimsSet.Builder baseClaims() {
    Map<String, Object> realmAccess = new LinkedHashMap<>();
    realmAccess.put("roles", Arrays.asList("writer", "read"));

    return new JWTClaimsSet.Builder()
        .issuer(ISSUER)
        .subject("subject-tomato")
        .audience(Collections.singletonList(AUDIENCE))
        .issueTime(Date.from(NOW.minusSeconds(30)))
        .expirationTime(Date.from(NOW.plusSeconds(3600)))
        .claim("preferred_username", "tomato-user")
        .claim("groups", Collections.singletonList("ozone-tomato"))
        .claim("realm_access", realmAccess);
  }

  @FunctionalInterface
  private interface ClaimsCustomizer {
    void customize(JWTClaimsSet.Builder builder);
  }
}
