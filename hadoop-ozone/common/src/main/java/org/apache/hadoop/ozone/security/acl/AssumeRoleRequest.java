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
import java.util.AbstractMap;
import java.util.Objects;
import java.util.Set;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Represents an S3 AssumeRole request that needs to be authorized by an IAccessAuthorizer.
 * The grants parameter can be obtained via a call to
 * {@link IamSessionPolicyResolver#resolve(String, String, IamSessionPolicyResolver.AuthorizerType)},
 * or it can be null if the access must not be limited beyond the role.
 */
@Immutable
public class AssumeRoleRequest {
  private final String host;
  private final InetAddress ip;
  private final UserGroupInformation clientUgi;
  private final String targetRoleName;
  private final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<IAccessAuthorizer.ACLType>>> grants;

  public AssumeRoleRequest(
      String host,
      InetAddress ip,
      UserGroupInformation clientUgi,
      String targetRoleName,
      Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<IAccessAuthorizer.ACLType>>> grants
  ) {

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

  public Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<IAccessAuthorizer.ACLType>>> getGrants() {
    return grants;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AssumeRoleRequest that = (AssumeRoleRequest) o;
    return Objects.equals(host, that.host) &&
        Objects.equals(ip, that.ip) &&
        Objects.equals(clientUgi, that.clientUgi) &&
        Objects.equals(targetRoleName, that.targetRoleName) &&
        Objects.equals(grants, that.grants);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, ip, clientUgi, targetRoleName, grants);
  }
}
