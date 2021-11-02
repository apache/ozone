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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_CLIENT_SSL_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
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

  public RangerClientMultiTenantAccessController(Configuration conf) {
    // TODO get these from the existing ranger plugin config.
    String rangerHttpsAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    System.err.println("Ranger address: " + rangerHttpsAddress);
    String clientSslFile = conf.get(OZONE_RANGER_CLIENT_SSL_FILE);
    System.err.println("ssl file: " + clientSslFile);
    service = conf.get(OZONE_RANGER_SERVICE);
    System.err.println("Ranger service name: " + service);

//    String principal = conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY);
//    String keytabPath = conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY);
//    client = new RangerClient(rangerHttpsAddress,
//        "kerberos", principal, keytabPath, clientSslFile);
    Configuration rangerConf = new Configuration();

    rangerConf.set("xasecure.policymgr.clientssl.truststore", "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks");
    rangerConf.set("xasecure.policymgr.clientssl.truststore.password", "/var/run/cloudera-scm-agent/process/52-ranger-RANGER_TAGSYNC/altscript.sh sec-0-xasecure.policymgr.clientssl.truststore.password");
    rangerConf.set("",
        "jceks://file/var/run/cloudera-scm-agent/process/52-ranger" +
            "-RANGER_TAGSYNC/conf/rangertagsyncssl.jceks");
//    rangerConf.set("xasecure.policymgr.clientssl.truststore.credential.file",
//        "jceks://file/var/run/cloudera-scm-agent/process/52-ranger" +
//            "-RANGER_TAGSYNC/conf/rangertagsyncssl.jceks");
    rangerConf.set("xasecure.policymgr.clientssl.truststore.credential.file",
        "localjceks://file//var/run/cloudera-scm-agent/process/29-ozone" +
            "-OZONE_MANAGER/creds.localjceks");
    rangerConf.set("xasecure.policymgr.clientssl.truststore.type", "jks");

    RangerRESTClient restClient = new RangerRESTClient(rangerHttpsAddress,
        clientSslFile, rangerConf);
    restClient.setBasicAuthInfo("admin", "admin123");
    client = new RangerClient(restClient);
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
      Collection<BasicUserPrincipal> newUsers) throws RangerServiceException {
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
    resource.put("volume",
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

    // Add roles to the policy.
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
    item.setRoles(policy.getRoles());
    rangerPolicy.getPolicyItems().add(item);
    return rangerPolicy;
  }

  private String toUserListString(Collection<BasicUserPrincipal> users) {
    return users.stream()
        .map(Object::toString).collect(Collectors.joining(", "));
  }
}
