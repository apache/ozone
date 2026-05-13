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
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Represents an S3 AssumeRole request that needs to be authorized by an IAccessAuthorizer.
 * The grants parameter can be null if the access must not be limited beyond the role.
 * Note that if the grants parameter is the empty set, this means the access should
 * be the intersection of the role and the empty set, meaning no access will be granted.
 */
@Immutable
public class AssumeRoleRequest {
  private final String host;
  private final InetAddress ip;
  private final UserGroupInformation clientUgi;
  private final String targetRoleName;
  private final Set<OzoneGrant> grants;

  public AssumeRoleRequest(String host, InetAddress ip, UserGroupInformation clientUgi, String targetRoleName,
      Set<OzoneGrant> grants) {

    this.host = host;
    this.ip = ip;
    this.clientUgi = clientUgi;
    this.targetRoleName = targetRoleName;
    this.grants = grants;
  }

  public String getHost() {
    return host;
  }

  public InetAddress getIp() {
    return ip;
  }

  public UserGroupInformation getClientUgi() {
    return clientUgi;
  }

  public String getTargetRoleName() {
    return targetRoleName;
  }

  public Set<OzoneGrant> getGrants() {
    return grants;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AssumeRoleRequest that = (AssumeRoleRequest) o;
    return Objects.equals(host, that.host) && Objects.equals(ip, that.ip) &&
        Objects.equals(clientUgi, that.clientUgi) && Objects.equals(targetRoleName, that.targetRoleName) &&
        Objects.equals(grants, that.grants);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, ip, clientUgi, targetRoleName, grants);
  }

  /**
   * Encapsulates the IOzoneObj and associated permissions.
   */
  @Immutable
  public static class OzoneGrant {
    private final Set<IOzoneObj> objects;
    private final Set<IAccessAuthorizer.ACLType> permissions;
    /**
     * S3 action names without the s3: prefix (e.g. GetObject) from the session policy.  When present, the permissions
     * will be further restricted by the set of available S3 actions.  An empty (or null) set means this OzoneGrant
     * does not enforce any restrictions on actions.
     */
    private final Set<String> s3Actions;

    public OzoneGrant(Set<IOzoneObj> objects, Set<IAccessAuthorizer.ACLType> permissions) {
      this.objects = objects;
      this.permissions = permissions;
      this.s3Actions = Collections.emptySet();
    }

    public OzoneGrant(Set<IOzoneObj> objects, Set<IAccessAuthorizer.ACLType> permissions,
        Set<String> s3Actions) {
      this.objects = objects;
      this.permissions = permissions;
      this.s3Actions = Collections.unmodifiableSet(new LinkedHashSet<>(s3Actions));
    }

    public Set<IOzoneObj> getObjects() {
      return objects;
    }

    public Set<IAccessAuthorizer.ACLType> getPermissions() {
      return permissions;
    }

    public Set<String> getS3Actions() {
      return s3Actions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final OzoneGrant that = (OzoneGrant) o;
      return Objects.equals(objects, that.objects) && Objects.equals(permissions, that.permissions) &&
          Objects.equals(s3Actions, that.s3Actions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(objects, permissions, s3Actions);
    }

    @Override
    public String toString() {
      return "OzoneGrant{" + "objects=" + objects + ", permissions=" + permissions + ", s3Actions="
          + s3Actions + '}';
    }
  }
}
