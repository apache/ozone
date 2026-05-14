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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.SignedJWT;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * OIDC identity provider backed by locally validated signed JWTs.
 */
public final class OidcJwtIdentityProvider implements OzoneIdentityProvider {

  private final OidcConfig config;
  private final JwksProvider jwksProvider;
  private final Clock clock;

  public OidcJwtIdentityProvider(OidcConfig config,
      JwksProvider jwksProvider) {
    this(config, jwksProvider, Clock.systemUTC());
  }

  OidcJwtIdentityProvider(OidcConfig config, JwksProvider jwksProvider,
      Clock clock) {
    if (config == null) {
      throw new IllegalArgumentException("OIDC config must not be null");
    }
    if (jwksProvider == null) {
      throw new IllegalArgumentException("JWKS provider must not be null");
    }
    config.validateForProvider();
    this.config = config;
    this.jwksProvider = jwksProvider;
    this.clock = clock;
  }

  @Override
  public OzoneIdentity authenticate(AuthCredentials credentials)
      throws OidcAuthenticationException {
    if (credentials == null || credentials.getBearerToken() == null
        || StringUtils.isBlank(credentials.getBearerToken())) {
      throw new OidcAuthenticationException("Missing OIDC bearer token");
    }

    SignedJWT jwt = parse(credentials.getBearerToken());
    verifySignature(jwt);
    JWTClaimsSet claims = claims(jwt);
    return toIdentity(claims);
  }

  private SignedJWT parse(String token) throws OidcAuthenticationException {
    try {
      JWT jwt = JWTParser.parse(token);
      if (!(jwt instanceof SignedJWT)) {
        throw new OidcAuthenticationException("OIDC token must be signed");
      }
      return (SignedJWT) jwt;
    } catch (ParseException e) {
      throw new OidcAuthenticationException("Unable to parse OIDC token");
    }
  }

  private void verifySignature(SignedJWT jwt)
      throws OidcAuthenticationException {
    JWSHeader header = jwt.getHeader();
    JWSAlgorithm algorithm = header.getAlgorithm();
    if (algorithm == null || "none".equalsIgnoreCase(algorithm.getName())) {
      throw new OidcAuthenticationException(
          "OIDC token must use a signed algorithm");
    }
    if (!JWSAlgorithm.Family.RSA.contains(algorithm)) {
      throw new OidcAuthenticationException(
          "Unsupported OIDC token signing algorithm");
    }

    List<JWK> keys = jwksProvider.getKeys(header.getKeyID());
    if (keys.isEmpty()) {
      throw new OidcAuthenticationException("OIDC signing key is unknown");
    }
    for (JWK key : keys) {
      if (verifyWithKey(jwt, algorithm, key)) {
        return;
      }
    }
    throw new OidcAuthenticationException(
        "OIDC token signature does not match");
  }

  private boolean verifyWithKey(SignedJWT jwt, JWSAlgorithm algorithm,
      JWK key) throws OidcAuthenticationException {
    if (!(key instanceof RSAKey)) {
      return false;
    }
    if (key.getAlgorithm() != null
        && !algorithm.getName().equals(key.getAlgorithm().getName())) {
      return false;
    }

    try {
      JWSVerifier verifier =
          new RSASSAVerifier(((RSAKey) key).toRSAPublicKey());
      return jwt.verify(verifier);
    } catch (JOSEException e) {
      throw new OidcAuthenticationException(
          "Unable to verify OIDC token signature", e);
    }
  }

  private JWTClaimsSet claims(SignedJWT jwt)
      throws OidcAuthenticationException {
    try {
      return jwt.getJWTClaimsSet();
    } catch (ParseException e) {
      throw new OidcAuthenticationException(
          "Unable to parse OIDC token claims");
    }
  }

  private OzoneIdentity toIdentity(JWTClaimsSet claims)
      throws OidcAuthenticationException {
    Instant now = clock.instant();
    Instant expiresAt = validateExpiration(claims, now);
    validateNotBefore(claims, now);
    Instant authenticatedAt = validateIssueTime(claims, now);
    validateIssuer(claims);
    validateAudience(claims);

    String username = extractStringClaim(claims, config.getUsernameClaim());
    String subject = extractStringClaim(claims, config.getSubjectClaim());
    Set<String> groups = extractStringSet(claims, config.getGroupsClaim());
    Set<String> roles = extractStringSet(claims, config.getRolesClaim());

    return OzoneIdentity.newBuilder()
        .setUsername(username)
        .setSubject(subject)
        .setIssuer(claims.getIssuer())
        .setGroups(groups)
        .setRoles(roles)
        .setAuthMethod(OzoneIdentity.AUTH_METHOD_OIDC)
        .setAuthenticatedAt(authenticatedAt)
        .setExpiresAt(expiresAt)
        .setRawClaims(claims.getClaims())
        .build();
  }

  private Instant validateExpiration(JWTClaimsSet claims, Instant now)
      throws OidcAuthenticationException {
    Date expiration = claims.getExpirationTime();
    if (expiration == null) {
      throw new OidcAuthenticationException(
          "OIDC token does not contain expiration");
    }
    Instant expiresAt = expiration.toInstant();
    if (expiresAt.plus(config.getClockSkew()).isBefore(now)) {
      throw new OidcAuthenticationException("OIDC token is expired");
    }
    return expiresAt;
  }

  private void validateNotBefore(JWTClaimsSet claims, Instant now)
      throws OidcAuthenticationException {
    Date notBefore = claims.getNotBeforeTime();
    if (notBefore != null) {
      Duration clockSkew = config.getClockSkew();
      if (notBefore.toInstant().minus(clockSkew).isAfter(now)) {
        throw new OidcAuthenticationException(
            "OIDC token is not valid yet");
      }
    }
  }

  private Instant validateIssueTime(JWTClaimsSet claims, Instant now)
      throws OidcAuthenticationException {
    Date issueTime = claims.getIssueTime();
    if (issueTime == null) {
      throw new OidcAuthenticationException(
          "OIDC token does not contain issue time");
    }
    Instant issuedAt = issueTime.toInstant();
    if (issuedAt.minus(config.getClockSkew()).isAfter(now)) {
      throw new OidcAuthenticationException(
          "OIDC token issue time is in the future");
    }
    return issuedAt;
  }

  private void validateIssuer(JWTClaimsSet claims)
      throws OidcAuthenticationException {
    if (!config.getIssuerUri().equals(claims.getIssuer())) {
      throw new OidcAuthenticationException(
          "OIDC token issuer does not match");
    }
  }

  private void validateAudience(JWTClaimsSet claims)
      throws OidcAuthenticationException {
    List<String> audiences = claims.getAudience();
    if (audiences == null || !audiences.contains(config.getAudience())) {
      throw new OidcAuthenticationException(
          "OIDC token audience does not match");
    }
  }

  private String extractStringClaim(JWTClaimsSet claims, String claimPath)
      throws OidcAuthenticationException {
    Object value = claimValue(claims, claimPath);
    if (!(value instanceof String) || StringUtils.isBlank((String) value)) {
      throw new OidcAuthenticationException(
          "OIDC token is missing required claim");
    }
    return ((String) value).trim();
  }

  private Set<String> extractStringSet(JWTClaimsSet claims, String claimPath)
      throws OidcAuthenticationException {
    Object value = claimValue(claims, claimPath);
    if (value == null) {
      return Collections.emptySet();
    }

    Set<String> result = new LinkedHashSet<>();
    if (value instanceof String) {
      addStringValue(result, (String) value);
    } else if (value instanceof Collection) {
      for (Object item : (Collection<?>) value) {
        addStringValue(result, item);
      }
    } else if (value.getClass().isArray()) {
      int length = Array.getLength(value);
      for (int i = 0; i < length; i++) {
        addStringValue(result, Array.get(value, i));
      }
    } else {
      throw new OidcAuthenticationException(
          "OIDC token claim has unsupported type");
    }
    return Collections.unmodifiableSet(result);
  }

  private void addStringValue(Set<String> values, Object value)
      throws OidcAuthenticationException {
    if (!(value instanceof String)) {
      throw new OidcAuthenticationException(
          "OIDC token claim contains non-string value");
    }
    String trimmed = ((String) value).trim();
    if (!trimmed.isEmpty()) {
      values.add(trimmed);
    }
  }

  private Object claimValue(JWTClaimsSet claims, String claimPath) {
    Object directValue = claims.getClaim(claimPath);
    if (directValue != null) {
      return directValue;
    }

    String[] parts = claimPath.split("\\.");
    Object current = claims.getClaim(parts[0]);
    for (int i = 1; i < parts.length; i++) {
      if (!(current instanceof Map)) {
        return null;
      }
      current = ((Map<?, ?>) current).get(parts[i]);
      if (current == null) {
        return null;
      }
    }
    return current;
  }
}
