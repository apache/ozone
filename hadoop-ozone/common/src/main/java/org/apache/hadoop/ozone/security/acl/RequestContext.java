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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class encapsulates information required for Ozone ACLs.
 * */
public class RequestContext {
  private final String host;
  private final InetAddress ip;
  private final UserGroupInformation clientUgi;
  private final String serviceId;
  private final ACLIdentityType aclType;
  private final ACLType aclRights;
  private final String ownerName;

  /**
   * Represents recursive access check required for all the sub-paths of the
   * given path. If the given path is not a directory, there is no effect for
   * this flag. A true value represents recursive check, false represents
   * non-recursive check.
   */
  private final boolean recursiveAccessCheck;

  /**
   * Represents optional session policy JSON for Ranger to use when authorizing
   * an STS token.  This value would have come as a result of a previous
   * {@link IAccessAuthorizer#generateAssumeRoleSessionPolicy(AssumeRoleRequest)} call.
   * The sessionPolicy includes the roleName.
   */
  private final String sessionPolicy;

  @SuppressWarnings("parameternumber")
  public RequestContext(String host, InetAddress ip,
      UserGroupInformation clientUgi, String serviceId,
      ACLIdentityType aclType, ACLType aclRights,
      String ownerName) {
    this(host, ip, clientUgi, serviceId, aclType, aclRights, ownerName,
        false, null);
  }

  @SuppressWarnings("parameternumber")
  public RequestContext(String host, InetAddress ip,
      UserGroupInformation clientUgi, String serviceId,
      ACLIdentityType aclType, ACLType aclRights,
      String ownerName, boolean recursiveAccessCheck) {
    this(host, ip, clientUgi, serviceId, aclType, aclRights, ownerName,
        recursiveAccessCheck, null);
  }

  @SuppressWarnings("parameternumber")
  public RequestContext(String host, InetAddress ip, UserGroupInformation clientUgi, String serviceId,
      ACLIdentityType aclType, ACLType aclRights, String ownerName, boolean recursiveAccessCheck,
      String sessionPolicy) {
    this.host = host;
    this.ip = ip;
    this.clientUgi = clientUgi;
    this.serviceId = serviceId;
    this.aclType = aclType;
    this.aclRights = aclRights;
    this.ownerName = ownerName;
    this.recursiveAccessCheck = recursiveAccessCheck;
    this.sessionPolicy = sessionPolicy;
  }

  /**
   * Builder class for @{@link RequestContext}.
   */
  public static class Builder {
    private String host;
    private InetAddress ip;
    private UserGroupInformation clientUgi;
    private String serviceId;
    private IAccessAuthorizer.ACLIdentityType aclType;
    private IAccessAuthorizer.ACLType aclRights;

    /**
     *  ownerName is specially added to allow
     *  authorizer to honor owner privilege.
     */
    private String ownerName;

    private boolean recursiveAccessCheck;
    private String sessionPolicy;

    public Builder setHost(String bHost) {
      this.host = bHost;
      return this;
    }

    public Builder setIp(InetAddress cIp) {
      this.ip = cIp;
      return this;
    }

    public Builder setClientUgi(UserGroupInformation cUgi) {
      this.clientUgi = cUgi;
      return this;
    }

    public Builder setServiceId(String sId) {
      this.serviceId = sId;
      return this;
    }

    public Builder setAclType(ACLIdentityType acl) {
      this.aclType = acl;
      return this;
    }

    public ACLType getAclRights() {
      return this.aclRights;
    }

    public Builder setAclRights(ACLType aclRight) {
      this.aclRights = aclRight;
      return this;
    }

    public Builder setOwnerName(String owner) {
      this.ownerName = owner;
      return this;
    }

    public Builder setRecursiveAccessCheck(boolean recursiveAccessCheckFlag) {
      this.recursiveAccessCheck = recursiveAccessCheckFlag;
      return this;
    }

    public Builder setSessionPolicy(String sessionPolicy) {
      this.sessionPolicy = sessionPolicy;
      return this;
    }

    public RequestContext build() {
      return new RequestContext(host, ip, clientUgi, serviceId, aclType,
          aclRights, ownerName, recursiveAccessCheck, sessionPolicy);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static RequestContext.Builder getBuilder(
      UserGroupInformation ugi, InetAddress remoteAddress, String hostName,
      ACLType aclType, String ownerName) {
    return getBuilder(ugi, remoteAddress, hostName, aclType, ownerName,
        false);
  }

  public static RequestContext.Builder getBuilder(
      UserGroupInformation ugi, InetAddress remoteAddress, String hostName,
      ACLType aclType, String ownerName, boolean recursiveAccessCheck) {
    return getBuilder(ugi, remoteAddress, hostName, aclType, ownerName, recursiveAccessCheck, null);
  }

  public static RequestContext.Builder getBuilder(UserGroupInformation ugi, InetAddress remoteAddress, String hostName,
      ACLType aclType, String ownerName, boolean recursiveAccessCheck, String sessionPolicy) {
    return RequestContext.newBuilder()
        .setClientUgi(ugi)
        .setIp(remoteAddress)
        .setHost(hostName)
        .setAclType(ACLIdentityType.USER)
        .setAclRights(aclType)
        .setOwnerName(ownerName)
        .setRecursiveAccessCheck(recursiveAccessCheck)
        .setSessionPolicy(sessionPolicy);
  }

  public static RequestContext.Builder getBuilder(UserGroupInformation ugi,
      ACLType aclType, String ownerName) {
    return getBuilder(ugi,
        ProtobufRpcEngine.Server.getRemoteIp(),
        ProtobufRpcEngine.Server.getRemoteIp().getHostName(),
        aclType, ownerName);
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

  public String getServiceId() {
    return serviceId;
  }

  public ACLIdentityType getAclType() {
    return aclType;
  }

  public ACLType getAclRights() {
    return aclRights;
  }

  public String getOwnerName() {
    return ownerName;
  }

  /**
   * A true value represents recursive access check required for all the
   * sub-paths of the given path, false represents non-recursive check.
   * <p>
   * If the given path is not a directory, there is no effect for this flag.
   */
  public boolean isRecursiveAccessCheck() {
    return recursiveAccessCheck;
  }

  public String getSessionPolicy() {
    return sessionPolicy;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setHost(host)
        .setIp(ip)
        .setClientUgi(clientUgi)
        .setServiceId(serviceId)
        .setAclType(aclType)
        .setAclRights(aclRights)
        .setOwnerName(ownerName)
        .setRecursiveAccessCheck(recursiveAccessCheck)
        .setSessionPolicy(sessionPolicy);
  }
}
