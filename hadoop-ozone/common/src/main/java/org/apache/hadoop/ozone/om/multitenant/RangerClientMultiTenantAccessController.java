/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.multitenant;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_CLIENT_SSL_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.ranger.RangerClient;

/**
 * Implementation of {@link MultiTenantAccessController} using the
 * {@link RangerClient} to communicate with Ranger.
 */
public class RangerClientMultiTenantAccessController implements
    MultiTenantAccessController {

  private static final Logger LOG = LoggerFactory
      .getLogger(MultiTenantAccessController.class);

  private final RangerClient client;
  private final String service;
  private final Map<IAccessAuthorizer.ACLType, String> rangerAclStrings;

  public RangerClientMultiTenantAccessController(OzoneConfiguration conf) {
    rangerAclStrings = new EnumMap<>(IAccessAuthorizer.ACLType.class);

    // TODO get these from the existing ranger plugin config.
    String rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    System.err.println("Ranger address: " + rangerHttpsAddress);
    service = conf.get(OZONE_RANGER_SERVICE);
    System.err.println("Ranger service name: " + service);

    // Auth using kerberos if using 3.0 snapshot ranger client.
    String principal = conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY);
    String keytabPath = conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY);
    client = new RangerClient(rangerHttpsAddress,
        "kerberos", principal, keytabPath, service, "ozone");

    // Auth with username/password if using 2.1.0 ranger client.
//    String username = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER);
//    String password = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD);
//    String clientSslFile = conf.get(OZONE_RANGER_CLIENT_SSL_FILE);
//    System.err.println("ssl file: " + clientSslFile);
//    RangerRESTClient restClient = new RangerRESTClient(rangerHttpsAddress,
//        clientSslFile, conf);
//    restClient.setBasicAuthInfo(username, password);
//    client = new RangerClient(restClient);
  }

  @Override
  public long createPolicy(Policy policy) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending create request for policy {} to Ranger.",
          policy.getName());
    }
    return client.createPolicy(toRangerPolicy(policy)).getId();
  }

  @Override
  public void enablePolicy(long policyID) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending enable request for policy ID {} to Ranger.",
          policyID);
    }
    RangerPolicy rangerPolicy = client.getPolicy(policyID);
    rangerPolicy.setIsEnabled(true);
    client.updatePolicy(policyID, rangerPolicy);
  }

  @Override
  public void disablePolicy(long policyID) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending disable request for policy ID {} to Ranger.",
          policyID);
    }
    RangerPolicy rangerPolicy = client.getPolicy(policyID);
    rangerPolicy.setIsEnabled(true);
    client.updatePolicy(policyID, rangerPolicy);
  }

  @Override
  public void deletePolicy(long policyID) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending delete request for policy ID {} to Ranger.",
          policyID);
    }
    client.deletePolicy(policyID);

  }

  @Override
  public long createRole(Role role) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending create request for role {} to Ranger.",
          role.getName());
    }
    return client.createRole(service, toRangerRole(role)).getId();
  }

  @Override
  public void deleteRole(long roleID) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending delete request for role ID {} to Ranger.",
          roleID);
    }
    client.deleteRole(roleID);
  }

  @Override
  public void addUsersToRole(long roleID,
      BasicUserPrincipal... newUsers) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding users {} to role ID {} in Ranger.",
          toUserListString(newUsers), roleID);
    }
    RangerRole originalRole = client.getRole(roleID);
    originalRole.getUsers().addAll(toRangerRoleMembers(newUsers));
    client.updateRole(roleID, originalRole);
  }

  @Override
  public void removeUsersFromRole(long roleID,
      BasicUserPrincipal... users)
      throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing users {} from role ID {} in Ranger.",
          toUserListString(users), roleID);
    }
    RangerRole originalRole = client.getRole(roleID);
    originalRole.getUsers().removeAll(toRangerRoleMembers(users));
    client.updateRole(roleID, originalRole);
  }

  private static List<RangerRole.RoleMember> toRangerRoleMembers(
      Collection<BasicUserPrincipal> users) {
    return users.stream()
            .map(princ -> new RangerRole.RoleMember(princ.getName(), false))
            .collect(Collectors.toList());
  }

  private static List<RangerRole.RoleMember> toRangerRoleMembers(
      BasicUserPrincipal[] users) {
    return Arrays.stream(users)
        .map(princ -> new RangerRole.RoleMember(princ.getName(), false))
        .collect(Collectors.toList());
  }

  private static RangerRole toRangerRole(Role role) {
    RangerRole rangerRole = new RangerRole();
    rangerRole.setName(role.getName());
    rangerRole.setUsers(toRangerRoleMembers(role.getUsers()));
    if (role.getDescription().isPresent()) {
      rangerRole.setDescription(role.getDescription().get());
    }
    return rangerRole;
  }

  private RangerPolicy toRangerPolicy(Policy policy) {
    RangerPolicy rangerPolicy = new RangerPolicy();
    rangerPolicy.setName(policy.getName());

    Map<String, RangerPolicy.RangerPolicyResource> resource = new HashMap<>();
    for (String volume: policy.getVolumes()) {
      resource.put("volume",
          new RangerPolicy.RangerPolicyResource(volume));
    }
    for (String bucket: policy.getVolumes()) {
      resource.put("bucket",
          new RangerPolicy.RangerPolicyResource(bucket));
    }
    for (String key: policy.getVolumes()) {
      resource.put("key",
          new RangerPolicy.RangerPolicyResource(key));
    }

    rangerPolicy.setService(service);
    rangerPolicy.setResources(resource);

    // Add roles to the policy.
    for (Map.Entry<String, Collection<Acl>> roleAcls:
        policy.getRoleAcls().entrySet()) {
      RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
      item.setRoles(Collections.singletonList(roleAcls.getKey()));

      for (Acl acl: roleAcls.getValue()) {
        RangerPolicy.RangerPolicyItemAccess access =
            new RangerPolicy.RangerPolicyItemAccess();
        access.setIsAllowed(acl.isAllowed());
        access.setType(rangerAclStrings.get(acl.getAclType()));
        item.getAccesses().add(access);
      }

      rangerPolicy.getPolicyItems().add(item);
    }

    return rangerPolicy;
  }

  private String toUserListString(BasicUserPrincipal[] users) {
    return Arrays.stream(users)
        .map(Object::toString).collect(Collectors.joining(", "));
  }

  private void fillRangerAclStrings() {
    rangerAclStrings.put(IAccessAuthorizer.ACLType.ALL, "All");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.LIST, "List");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ, "Read");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE, "Write");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.CREATE, "Create");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.DELETE, "Delete");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ_ACL, "Read_ACL");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE_ACL, "Write_ACL");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.NONE, "");
  }
}
