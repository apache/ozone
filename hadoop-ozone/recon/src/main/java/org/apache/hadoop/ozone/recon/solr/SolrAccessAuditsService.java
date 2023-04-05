/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.solr;

import com.google.inject.Inject;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.recon.api.types.EntityReadAccessHeatMapResponse;
import org.apache.hadoop.ozone.recon.http.ReconHttpClient;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * This class is an implementation of abstract class for querying entity's
 * audit logs through SolrAccessAuditService.
 */
public class SolrAccessAuditsService extends AccessAuditsService {
  private static final Logger LOG =
      LoggerFactory.getLogger(SolrAccessAuditsService.class);
  private UserGroupInformation reconUser;
  private final OzoneConfiguration ozoneConfiguration;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final OzoneStorageContainerManager reconSCM;
  private ReconHttpClient reconHttpClient;

  @Inject
  public SolrAccessAuditsService(OzoneConfiguration ozoneConfiguration,
                                 ReconNamespaceSummaryManager
                                     namespaceSummaryManager,
                                 ReconOMMetadataManager omMetadataManager,
                                 OzoneStorageContainerManager reconSCM,
                                 ReconHttpClient reconHttpClient) {
    this.reconHttpClient = reconHttpClient;
    this.ozoneConfiguration = ozoneConfiguration;
    this.reconNamespaceSummaryManager = namespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
    this.reconUser = initializeReconUGI(ozoneConfiguration);
  }

  /**
   * Initialize recon UGI if security is enabled in the configuration.
   *
   * @param conf OzoneConfiguration
   * @return recon UserGroupInformation object
   */
  private static UserGroupInformation initializeReconUGI(
      OzoneConfiguration conf) {
    try {
      if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
        return getReconUGI(conf);
      }
    } catch (Exception ex) {
      LOG.error("Error initializing recon UGI. ", ex);
    }
    return null;
  }

  /**
   * Gets recon UGI if security is enabled.
   *
   * @param conf
   * @return recon UserGroupInformation object
   */
  private static UserGroupInformation getReconUGI(
      OzoneConfiguration conf) throws AuthenticationException, IOException {
    UserGroupInformation reconUGI;
    if (SecurityUtil.getAuthenticationMethod(conf).equals(
        UserGroupInformation.AuthenticationMethod.KERBEROS)) {
      ReconConfig reconConfig = conf.getObject(ReconConfig.class);
      String principalUser = reconConfig.getKerberosPrincipal();
      String kerberosKeyTabFile = reconConfig.getKerberosKeytab();
      LOG.info("Ozone security is enabled. Attempting to get UGI for "
              + "recon user. Principal: {}, keytab: {}",
          principalUser,
          kerberosKeyTabFile);
      UserGroupInformation.setConfiguration(conf);

      InetSocketAddress socAddr = HddsUtils.getSolrAddress(conf);
      LOG.info("Get principal config for Solr host address: {} " + socAddr);
      String principalConfig = conf.get(OZONE_RECON_KERBEROS_PRINCIPAL_KEY,
          System.getProperty("user.name"));
      String principalName = SecurityUtil.getServerPrincipal(
          principalConfig, socAddr.getHostName());
      LOG.info("Principal name for  for Solr host address: {} " +
          principalName);
      reconUGI = UserGroupInformation.
          loginUserFromKeytabAndReturnUGI(principalName, kerberosKeyTabFile);
      LOG.info("recon UGI initialization successful. Auth  Method: {}," +
              "  UserName: {}" + reconUGI.getAuthenticationMethod(),
          reconUGI.getUserName());
    } else {
      reconUGI = null;
      throw new AuthenticationException(
          SecurityUtil.getAuthenticationMethod(conf) +
              " authentication method not supported. Recon user " +
              "initialization failed.");
    }
    return reconUGI;
  }

  @Override
  public EntityReadAccessHeatMapResponse querySolrForOzoneAudit(
      String path,
      String entityType,
      String startDate) {
    SolrUtil solrUtil = new SolrUtil(
        reconNamespaceSummaryManager, omMetadataManager, reconSCM,
        ozoneConfiguration);
    solrUtil.queryLogs(normalizePath(path), entityType, startDate,
        reconHttpClient);
    return solrUtil.getEntityReadAccessHeatMapResponse();
  }

  private String normalizePath(String path) {
    if (null != path && path.startsWith(OM_KEY_PREFIX)) {
      path = path.substring(1);
    }
    return path;
  }
}
