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

package org.apache.hadoop.ozone.security.acl;

import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.StringUtils;

/**
 * Represents an STS AssumeRoleWithWebIdentity request that has already been
 * authenticated by validating the web identity token.
 *
 * The web identity token itself must not be stored in this object. OM is the
 * authoritative validator for the token and passes only normalized identity
 * attributes to the authorizer.
 */
@Immutable
public class AssumeRoleWithWebIdentityRequest {

  public static final String ACTION = "AssumeRoleWithWebIdentity";

  private final String host;
  private final InetAddress ip;
  private final String user;
  private final Set<String> groups;
  private final Set<String> roles;
  private final String roleArn;
  private final String roleSessionName;
  private final String issuer;
  private final String subject;
  private final String audience;
  private final String providerId;
  private final Set<AssumeRoleRequest.OzoneGrant> grants;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public AssumeRoleWithWebIdentityRequest(String host, InetAddress ip,
      String user, Set<String> groups, Set<String> roles, String roleArn,
      String roleSessionName, String issuer, String subject, String audience,
      String providerId, Set<AssumeRoleRequest.OzoneGrant> grants) {
    this.host = host;
    this.ip = ip;
    this.user = requireNonBlank(user, "user");
    this.groups = immutableSet(groups);
    this.roles = immutableSet(roles);
    this.roleArn = requireNonBlank(roleArn, "roleArn");
    this.roleSessionName =
        requireNonBlank(roleSessionName, "roleSessionName");
    this.issuer = requireNonBlank(issuer, "issuer");
    this.subject = requireNonBlank(subject, "subject");
    this.audience = requireNonBlank(audience, "audience");
    this.providerId = providerId;
    this.grants = grants;
  }

  public String getAction() {
    return ACTION;
  }

  public String getHost() {
    return host;
  }

  public InetAddress getIp() {
    return ip;
  }

  public String getUser() {
    return user;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public String getRoleArn() {
    return roleArn;
  }

  public String getRoleSessionName() {
    return roleSessionName;
  }

  public String getIssuer() {
    return issuer;
  }

  public String getSubject() {
    return subject;
  }

  public String getAudience() {
    return audience;
  }

  public String getProviderId() {
    return providerId;
  }

  public Set<AssumeRoleRequest.OzoneGrant> getGrants() {
    return grants;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AssumeRoleWithWebIdentityRequest that =
        (AssumeRoleWithWebIdentityRequest) o;
    return Objects.equals(host, that.host)
        && Objects.equals(ip, that.ip)
        && Objects.equals(user, that.user)
        && Objects.equals(groups, that.groups)
        && Objects.equals(roles, that.roles)
        && Objects.equals(roleArn, that.roleArn)
        && Objects.equals(roleSessionName, that.roleSessionName)
        && Objects.equals(issuer, that.issuer)
        && Objects.equals(subject, that.subject)
        && Objects.equals(audience, that.audience)
        && Objects.equals(providerId, that.providerId)
        && Objects.equals(grants, that.grants);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, ip, user, groups, roles, roleArn,
        roleSessionName, issuer, subject, audience, providerId, grants);
  }

  @Override
  public String toString() {
    return "AssumeRoleWithWebIdentityRequest{"
        + "host='" + host + '\''
        + ", ip=" + ip
        + ", user='" + user + '\''
        + ", groups=" + groups
        + ", roles=" + roles
        + ", roleArn='" + roleArn + '\''
        + ", roleSessionName='" + roleSessionName + '\''
        + ", issuer='" + issuer + '\''
        + ", subject='" + subject + '\''
        + ", audience='" + audience + '\''
        + ", providerId='" + providerId + '\''
        + ", grants=" + grants
        + '}';
  }

  private static Set<String> immutableSet(Set<String> values) {
    if (values == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(new LinkedHashSet<>(values));
  }

  private static String requireNonBlank(String value, String name) {
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return value;
  }
}
