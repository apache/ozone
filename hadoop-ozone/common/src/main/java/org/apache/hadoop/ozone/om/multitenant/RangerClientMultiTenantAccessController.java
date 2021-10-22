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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.ranger.RangerClient;

/**
 * Implementation of {@link MultiTenantAccessController} using the
 * {@link RangerClient}.
 */
public class RangerClientMultiTenantAccessController implements
    MultiTenantAccessController {

  private static final Logger LOG = LoggerFactory
      .getLogger(MultiTenantAccessController.class);

  private OzoneConfiguration conf;
  private String rangerHttpsAddress;
  private RangerClient client;
  private String service;

  public RangerClientMultiTenantAccessController(Configuration configuration) {
    conf = new OzoneConfiguration(configuration);

    // TODO get these from the existing ranger plugin config.
    rangerHttpsAddress = "";
    service = "";

    String principal = conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY);
    String keytabPath = conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY);
    // TODO Pass path to ssl-client.xml config file if ssl is enabled.
    client = new RangerClient(rangerHttpsAddress,
        "kerberos", principal, keytabPath, null);
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
  public void enablePolicy(String policyName) throws RangerServiceException {
    // TODO may not be implemented in client.
  }

  @Override
  public void disablePolicy(String policyName) throws RangerServiceException {
    // TODO may not be implemented in client.
  }

  @Override
  public void deletePolicy(long policyID)
      throws RangerServiceException {
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
      Collection<BasicUserPrincipal> newUsers)
      throws RangerServiceException {
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
      Collection<BasicUserPrincipal> users)
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
    // TODO check if these are correct key strings in Ranger.
    resource.put("Ozone Volume",
        new RangerPolicy.RangerPolicyResource(policy.getVolume()));

    if (policy.getBucket().isPresent()) {
      resource.put("bucket",
          new RangerPolicy.RangerPolicyResource(policy.getBucket().get()));
    }
    if (policy.getKey().isPresent()) {
      resource.put("key",
          new RangerPolicy.RangerPolicyResource(policy.getKey().get()));
    }

    rangerPolicy.setService(service);
    rangerPolicy.setResources(resource);

    // TODO: Add roles to policy. Not currently supported by client.
    return rangerPolicy;
  }

  private String toUserListString(Collection<BasicUserPrincipal> users) {
    return users.stream()
        .map(Object::toString).collect(Collectors.joining(", "));
  }
}
