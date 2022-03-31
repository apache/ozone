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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
      .getLogger(RangerClientMultiTenantAccessController.class);

  private final RangerClient client;
  private final String rangerServiceName;
  private final Map<IAccessAuthorizer.ACLType, String> aclToString;
  private final Map<String, IAccessAuthorizer.ACLType> stringToAcl;
  private final String omPrincipal;

  public RangerClientMultiTenantAccessController(OzoneConfiguration conf) {
    aclToString = MultiTenantAccessController.getRangerAclStrings();
    stringToAcl = new HashMap<>();
    aclToString.forEach((type, string) -> stringToAcl.put(string, type));

    // TODO: Handle config setup in cluster without kerberos/tls/ranger.
    String rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    rangerServiceName = conf.get(OZONE_RANGER_SERVICE);
    omPrincipal = conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY);
    String keytabPath = conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY);
    client = new RangerClient(rangerHttpsAddress,
        "kerberos", omPrincipal, keytabPath, rangerServiceName, "ozone");
  }

  @Override
  public void createPolicy(Policy policy) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending create request for policy {} to Ranger.",
          policy.getName());
    }
    client.createPolicy(toRangerPolicy(policy));
  }

  @Override
  public Policy getPolicy(String policyName) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending get request for policy {} to Ranger.",
          policyName);
    }
    return fromRangerPolicy(client.getPolicy(rangerServiceName, policyName));
  }

  @Override
  public List<Policy> getLabeledPolicies(String label)
      throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending get request for policies with label {} to Ranger.",
          label);
    }
    // TODO: See if this actually gets policies by label.
    Map<String, String> filterMap = new HashMap<>();
    filterMap.put("policyLabels", label);
    return client.findPolicies(filterMap).stream()
        .map(this::fromRangerPolicy)
        .collect(Collectors.toList());
  }

  @Override
  public void updatePolicy(Policy policy) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending update request for policy {} to Ranger.",
          policy.getName());
    }
    client.updatePolicy(rangerServiceName, policy.getName(),
        toRangerPolicy(policy));
  }

  @Override
  public void deletePolicy(String policyName) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending delete request for policy {} to Ranger.",
          policyName);
    }
    client.deletePolicy(rangerServiceName, policyName);
  }

  @Override
  public void createRole(Role role) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending create request for role {} to Ranger.",
          role.getName());
    }
    client.createRole(rangerServiceName, toRangerRole(role));
  }

  @Override
  public Role getRole(String roleName) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending get request for role {} to Ranger.",
          roleName);
    }
    return fromRangerRole(client.getRole(roleName, omPrincipal,
        rangerServiceName));
  }

  @Override
  public void updateRole(long roleID, Role role) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending update request for role ID {} to Ranger.",
          roleID);
    }
    client.updateRole(roleID, toRangerRole(role));
  }

  @Override
  public void deleteRole(String roleName) throws RangerServiceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending delete request for role {} to Ranger.",
          roleName);
    }
    client.deleteRole(roleName, omPrincipal, rangerServiceName);
  }

  private static List<RangerRole.RoleMember> toRangerRoleMembers(
      Collection<BasicUserPrincipal> members) {
    return members.stream()
            .map(princ -> new RangerRole.RoleMember(princ.getName(), false))
            .collect(Collectors.toList());
  }

  private static List<BasicUserPrincipal> fromRangerRoleMembers(
      Collection<RangerRole.RoleMember> members) {
    return members.stream()
        .map(rangerUser -> new BasicUserPrincipal(rangerUser.getName()))
        .collect(Collectors.toList());
  }

  private static Role fromRangerRole(RangerRole rangerRole) {
    return new Role.Builder()
      .setID(rangerRole.getId())
      .setName(rangerRole.getName())
      .setDescription(rangerRole.getDescription())
      .addUsers(fromRangerRoleMembers(rangerRole.getUsers()))
      .build();
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

  private Policy fromRangerPolicy(RangerPolicy rangerPolicy) {
    Policy.Builder policyBuilder = new Policy.Builder();

    // Get roles and their acls from the policy.
    for (RangerPolicy.RangerPolicyItem policyItem:
        rangerPolicy.getPolicyItems()) {
      Collection<Acl> acls = new ArrayList<>();
      for (RangerPolicy.RangerPolicyItemAccess access:
           policyItem.getAccesses()) {
        if (access.getIsAllowed()) {
          acls.add(Acl.allow(stringToAcl.get(access.getType())));
        } else {
          acls.add(Acl.deny(stringToAcl.get(access.getType())));
        }
      }

      for (String roleName: policyItem.getRoles()) {
        policyBuilder.addRoleAcl(roleName, acls);
      }
    }

    // Add resources.
    for (Map.Entry<String, RangerPolicy.RangerPolicyResource> resource:
        rangerPolicy.getResources().entrySet()) {
      String resourceType = resource.getKey();
      List<String> resourceNames = resource.getValue().getValues();
      switch (resourceType) {
        case "volume":
          policyBuilder.addVolumes(resourceNames);
          break;
        case "bucket":
          policyBuilder.addBuckets(resourceNames);
          break;
        case "key":
          policyBuilder.addKeys(resourceNames);
          break;
        default:
          LOG.warn("Pulled Ranger policy with unknown resource type '{}' with" +
              " names '{}'", resourceType,
              String.join(",", resourceNames));
      }
    }

    policyBuilder.setName(rangerPolicy.getName())
       .setDescription(rangerPolicy.getDescription())
       .addLabels(rangerPolicy.getPolicyLabels());

    return policyBuilder.build();
  }

  private RangerPolicy toRangerPolicy(Policy policy) {
    RangerPolicy rangerPolicy = new RangerPolicy();
    rangerPolicy.setName(policy.getName());
    rangerPolicy.setService(rangerServiceName);
    rangerPolicy.setPolicyLabels(new ArrayList<>(policy.getLabels()));

    // Add resources.
    Map<String, RangerPolicy.RangerPolicyResource> resource = new HashMap<>();
    // Add volumes.
    RangerPolicy.RangerPolicyResource volumeResources =
        new RangerPolicy.RangerPolicyResource();
    volumeResources.setValues(new ArrayList<>(policy.getVolumes()));
    resource.put("volume", volumeResources);
    // Add buckets.
    RangerPolicy.RangerPolicyResource bucketResources =
        new RangerPolicy.RangerPolicyResource();
    bucketResources.setValues(new ArrayList<>(policy.getBuckets()));
    resource.put("bucket", bucketResources);
    // Add keys.
    RangerPolicy.RangerPolicyResource keyResources =
        new RangerPolicy.RangerPolicyResource();
    keyResources.setValues(new ArrayList<>(policy.getKeys()));
    resource.put("key", keyResources);

    rangerPolicy.setService(rangerServiceName);
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
        access.setType(aclToString.get(acl.getAclType()));
        item.getAccesses().add(access);
      }

      rangerPolicy.getPolicyItems().add(item);
    }

    return rangerPolicy;
  }
}
