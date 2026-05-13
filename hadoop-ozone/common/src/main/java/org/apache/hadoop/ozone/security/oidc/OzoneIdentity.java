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

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Normalized identity produced by an external authentication provider.
 */
public final class OzoneIdentity {

  public static final String AUTH_METHOD_OIDC = "oidc";

  private final String username;
  private final String subject;
  private final String issuer;
  private final Set<String> groups;
  private final Set<String> roles;
  private final String authMethod;
  private final Instant authenticatedAt;
  private final Instant expiresAt;
  private final Map<String, Object> rawClaims;

  private OzoneIdentity(Builder builder) {
    this.username = requireNonBlank(builder.username, "username");
    this.subject = requireNonBlank(builder.subject, "subject");
    this.issuer = requireNonBlank(builder.issuer, "issuer");
    this.groups = immutableSet(builder.groups);
    this.roles = immutableSet(builder.roles);
    this.authMethod = requireNonBlank(builder.authMethod, "authMethod");
    this.authenticatedAt = requireNonNull(
        builder.authenticatedAt, "authenticatedAt");
    this.expiresAt = requireNonNull(builder.expiresAt, "expiresAt");
    this.rawClaims = Collections.unmodifiableMap(
        new LinkedHashMap<>(builder.rawClaims));
  }

  public String getUsername() {
    return username;
  }

  public String getSubject() {
    return subject;
  }

  public String getIssuer() {
    return issuer;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public String getAuthMethod() {
    return authMethod;
  }

  public Instant getAuthenticatedAt() {
    return authenticatedAt;
  }

  public Instant getExpiresAt() {
    return expiresAt;
  }

  public Map<String, Object> getRawClaims() {
    return rawClaims;
  }

  @Override
  public String toString() {
    return "OzoneIdentity{"
        + "username='" + username + '\''
        + ", subject='" + subject + '\''
        + ", issuer='" + issuer + '\''
        + ", groups=" + groups
        + ", roles=" + roles
        + ", authMethod='" + authMethod + '\''
        + ", authenticatedAt=" + authenticatedAt
        + ", expiresAt=" + expiresAt
        + '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private static Set<String> immutableSet(Set<String> values) {
    return Collections.unmodifiableSet(new LinkedHashSet<>(values));
  }

  private static String requireNonBlank(String value, String name) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return value;
  }

  private static <T> T requireNonNull(T value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " must not be null");
    }
    return value;
  }

  /**
   * Builder for {@link OzoneIdentity}.
   */
  public static final class Builder {

    private String username;
    private String subject;
    private String issuer;
    private Set<String> groups = new LinkedHashSet<>();
    private Set<String> roles = new LinkedHashSet<>();
    private String authMethod = AUTH_METHOD_OIDC;
    private Instant authenticatedAt;
    private Instant expiresAt;
    private Map<String, Object> rawClaims = new LinkedHashMap<>();

    private Builder() {
    }

    public Builder setUsername(String value) {
      this.username = value;
      return this;
    }

    public Builder setSubject(String value) {
      this.subject = value;
      return this;
    }

    public Builder setIssuer(String value) {
      this.issuer = value;
      return this;
    }

    public Builder setGroups(Set<String> value) {
      this.groups = new LinkedHashSet<>(value);
      return this;
    }

    public Builder setRoles(Set<String> value) {
      this.roles = new LinkedHashSet<>(value);
      return this;
    }

    public Builder setAuthMethod(String value) {
      this.authMethod = value;
      return this;
    }

    public Builder setAuthenticatedAt(Instant value) {
      this.authenticatedAt = value;
      return this;
    }

    public Builder setExpiresAt(Instant value) {
      this.expiresAt = value;
      return this;
    }

    public Builder setRawClaims(Map<String, Object> value) {
      this.rawClaims = new LinkedHashMap<>(value);
      return this;
    }

    public OzoneIdentity build() {
      return new OzoneIdentity(this);
    }
  }
}
