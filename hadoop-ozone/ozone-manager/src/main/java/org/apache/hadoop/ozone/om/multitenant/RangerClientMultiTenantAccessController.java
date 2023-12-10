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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerService;
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

  private static final int HTTP_STATUS_CODE_UNAUTHORIZED = 401;
  private static final int HTTP_STATUS_CODE_BAD_REQUEST = 400;

  private final RangerClient client;
  private final String rangerServiceName;
  private final Map<IAccessAuthorizer.ACLType, String> aclToString;
  private final Map<String, IAccessAuthorizer.ACLType> stringToAcl;
  private final String omPrincipal;
  // execUser for Ranger
  private final String shortName;

  public RangerClientMultiTenantAccessController(OzoneConfiguration conf)
      throws IOException {

    aclToString = MultiTenantAccessController.getRangerAclStrings();
    stringToAcl = new HashMap<>();
    aclToString.forEach((type, string) -> stringToAcl.put(string, type));

    // Should have passed the config checks in
    // OMMultiTenantManager#checkAndEnableMultiTenancy at this point.

    String rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    Preconditions.checkNotNull(rangerHttpsAddress);
    rangerServiceName = conf.get(OZONE_RANGER_SERVICE);
    Preconditions.checkNotNull(rangerServiceName);

    // Determine auth type (KERBEROS or SIMPLE)
    final String authType;
    final String usernameOrPrincipal;
    final String passwordOrKeytab;

    // If both OZONE_OM_RANGER_HTTPS_ADMIN_API_USER and
    //  OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD are set, SIMPLE auth will be used
    String fallbackUsername = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER);
    String fallbackPassword = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD);

    if (fallbackUsername != null && fallbackPassword != null) {
      // Both clear text username and password are set, use SIMPLE auth.
      authType = AuthenticationMethod.SIMPLE.name();

      usernameOrPrincipal = fallbackUsername;
      passwordOrKeytab = fallbackPassword;

      omPrincipal = fallbackUsername;
      shortName = fallbackUsername;
    } else {
      // Use KERBEROS auth.
      authType = AuthenticationMethod.KERBEROS.name();

      String configuredOmPrincipal = conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY);
      Preconditions.checkNotNull(configuredOmPrincipal);

      // Replace _HOST pattern with host name in the Kerberos principal.
      // Ranger client currently does not do this automatically.
      omPrincipal = SecurityUtil.getServerPrincipal(
          configuredOmPrincipal, OmUtils.getOmAddress(conf).getHostName());
      final String keytabPath = conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY);
      Preconditions.checkNotNull(keytabPath);

      // Convert to short name to be used in some Ranger requests
      shortName = UserGroupInformation.createRemoteUser(omPrincipal)
          .getShortUserName();

      usernameOrPrincipal = omPrincipal;
      passwordOrKeytab = keytabPath;
    }

    LOG.info("authType = {}, login user = {}", authType, usernameOrPrincipal);

    client = new RangerClient(rangerHttpsAddress,
        authType, usernameOrPrincipal, passwordOrKeytab,
        rangerServiceName, OzoneConsts.OZONE);

    // Whether or not the Ranger credentials are valid is unknown right after
    // RangerClient initialization here. Because RangerClient does not perform
    // any authentication at this point just yet.
    //
    // If the credentials are invalid, RangerClient later throws 401 in every
    // single request to Ranger.
  }

  /**
   * Check StatusCode from RangerServiceException and try to log helpful,
   * actionable messages.
   *
   * @param rse RangerServiceException
   */
  private void decodeRSEStatusCodes(RangerServiceException rse) {
    ClientResponse.Status status = rse.getStatus();
    if (status == null) {
      LOG.error("Request failure with no status provided.", rse);
    } else {
      switch (status.getStatusCode()) {
      case HTTP_STATUS_CODE_UNAUTHORIZED:
        LOG.error("Auth failure. Please double check Ranger-related configs");
        break;
      case HTTP_STATUS_CODE_BAD_REQUEST:
        LOG.error("Request failure. If this is an assign-user operation, "
            + "check if the user name exists in Ranger.");
        break;
      default:
        LOG.error("Other request failure. Status: {}", status);
      }
    }
  }

  @Override
  public Policy createPolicy(Policy policy) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending create request for policy {} to Ranger.",
          policy.getName());
    }
    RangerPolicy rangerPolicy;
    try {
      rangerPolicy = client.createPolicy(toRangerPolicy(policy));
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
    return fromRangerPolicy(rangerPolicy);
  }

  @Override
  public Policy getPolicy(String policyName) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending get request for policy {} to Ranger.",
          policyName);
    }
    final RangerPolicy rangerPolicy;
    try {
      rangerPolicy = client.getPolicy(rangerServiceName, policyName);
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
    return fromRangerPolicy(rangerPolicy);
  }

  @Override
  public List<Policy> getLabeledPolicies(String label) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending get request for policies with label {} to Ranger.",
          label);
    }
    Map<String, String> filterMap = new HashMap<>();
    filterMap.put("serviceName", rangerServiceName);
    filterMap.put("policyLabelsPartial", label);
    try {
      return client.findPolicies(filterMap).stream()
          .map(this::fromRangerPolicy)
          .collect(Collectors.toList());
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
  }

  @Override
  public Policy updatePolicy(Policy policy) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending update request for policy {} to Ranger.",
          policy.getName());
    }
    final RangerPolicy rangerPolicy;
    try {
      rangerPolicy = client.updatePolicy(rangerServiceName,
          policy.getName(), toRangerPolicy(policy));
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
    return fromRangerPolicy(rangerPolicy);
  }

  @Override
  public void deletePolicy(String policyName) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending delete request for policy {} to Ranger.",
          policyName);
    }
    try {
      client.deletePolicy(rangerServiceName, policyName);
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
  }

  @Override
  public Role createRole(Role role) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending create request for role {} to Ranger.",
          role.getName());
    }
    final RangerRole rangerRole;
    try {
      rangerRole = client.createRole(rangerServiceName,
          toRangerRole(role, shortName));
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
    return fromRangerRole(rangerRole);
  }

  @Override
  public Role getRole(String roleName) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending get request for role {} to Ranger.",
          roleName);
    }
    final RangerRole rangerRole;
    try {
      rangerRole = client.getRole(roleName, shortName, rangerServiceName);
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
    return fromRangerRole(rangerRole);
  }

  @Override
  public Role updateRole(long roleId, Role role) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending update request for role ID {} to Ranger.",
          roleId);
    }
    // TODO: Check if createdByUser is even needed for updateRole request.
    //  If not, remove the createdByUser param and set it after.
    final RangerRole rangerRole;
    try {
      rangerRole = client.updateRole(roleId, toRangerRole(role, shortName));
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
    return fromRangerRole(rangerRole);
  }

  @Override
  public void deleteRole(String roleName) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending delete request for role {} to Ranger.",
          roleName);
    }
    try {
      client.deleteRole(roleName, shortName, rangerServiceName);
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
  }

  @Override
  public long getRangerServicePolicyVersion() throws IOException {
    RangerService rangerOzoneService;
    try {
      rangerOzoneService = client.getService(rangerServiceName);
    } catch (RangerServiceException e) {
      decodeRSEStatusCodes(e);
      throw new IOException(e);
    }
    // If the login user doesn't have sufficient privilege, policyVersion
    // field could be null in RangerService.
    final Long policyVersion = rangerOzoneService.getPolicyVersion();
    return policyVersion == null ? -1L : policyVersion;
  }

  private static List<RangerRole.RoleMember> toRangerRoleMembers(
      Map<String, Boolean> members) {
    return members.entrySet().stream()
            .map(entry -> {
              final String princ = entry.getKey();
              final boolean isRoleAdmin = entry.getValue();
              return new RangerRole.RoleMember(princ, isRoleAdmin);
            })
            .collect(Collectors.toList());
  }

  private static List<String> fromRangerRoleMembers(
      Collection<RangerRole.RoleMember> members) {
    return members.stream()
        .map(rangerUser -> rangerUser.getName())
        .collect(Collectors.toList());
  }

  private static Role fromRangerRole(RangerRole rangerRole) {
    return new Role.Builder()
      .setID(rangerRole.getId())
      .setName(rangerRole.getName())
      .setDescription(rangerRole.getDescription())
      .addUsers(fromRangerRoleMembers(rangerRole.getUsers()))
      .setCreatedByUser(rangerRole.getCreatedByUser())
      .build();
  }

  private static RangerRole toRangerRole(Role role, String createdByUser) {
    RangerRole rangerRole = new RangerRole();
    rangerRole.setName(role.getName());
    rangerRole.setCreatedByUser(createdByUser);
    if (!role.getUsersMap().isEmpty()) {
      rangerRole.setUsers(toRangerRoleMembers(role.getUsersMap()));
    }
    if (!role.getRolesMap().isEmpty()) {
      rangerRole.setRoles(toRangerRoleMembers(role.getRolesMap()));
    }
    if (role.getDescription().isPresent()) {
      rangerRole.setDescription(role.getDescription().get());
    }
    return rangerRole;
  }

  private Policy fromRangerPolicy(RangerPolicy rangerPolicy) {
    Policy.Builder policyBuilder = new Policy.Builder();

    // Get roles and their ACLs from the policy.
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
            " names '{}'", resourceType, String.join(",", resourceNames));
      }
    }

    policyBuilder.setName(rangerPolicy.getName())
        .setId(rangerPolicy.getId())
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
    if (!policy.getVolumes().isEmpty()) {
      RangerPolicy.RangerPolicyResource volumeResources =
          new RangerPolicy.RangerPolicyResource();
      volumeResources.setValues(new ArrayList<>(policy.getVolumes()));
      resource.put("volume", volumeResources);
    }
    // Add buckets.
    if (!policy.getBuckets().isEmpty()) {
      RangerPolicy.RangerPolicyResource bucketResources =
          new RangerPolicy.RangerPolicyResource();
      bucketResources.setValues(new ArrayList<>(policy.getBuckets()));
      resource.put("bucket", bucketResources);
    }
    // Add keys.
    if (!policy.getKeys().isEmpty()) {
      RangerPolicy.RangerPolicyResource keyResources =
          new RangerPolicy.RangerPolicyResource();
      keyResources.setValues(new ArrayList<>(policy.getKeys()));
      resource.put("key", keyResources);
    }
    rangerPolicy.setService(rangerServiceName);
    rangerPolicy.setResources(resource);
    if (policy.getDescription().isPresent()) {
      rangerPolicy.setDescription(policy.getDescription().get());
    }

    // Add users to the policy.
    for (Map.Entry<String, Collection<Acl>> userAcls:
        policy.getUserAcls().entrySet()) {
      RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
      item.setUsers(Collections.singletonList(userAcls.getKey()));

      for (Acl acl: userAcls.getValue()) {
        RangerPolicy.RangerPolicyItemAccess access =
            new RangerPolicy.RangerPolicyItemAccess();
        access.setIsAllowed(acl.isAllowed());
        access.setType(aclToString.get(acl.getAclType()));
        item.getAccesses().add(access);
      }

      rangerPolicy.getPolicyItems().add(item);
    }

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
