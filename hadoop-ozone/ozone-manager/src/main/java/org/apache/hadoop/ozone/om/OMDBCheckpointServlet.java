/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import javax.servlet.ServletException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * Provides the current checkpoint Snapshot of the OM DB. (tar.gz)
 *
 * When Ozone ACL is enabled (`ozone.acl.enabled`=`true`), only users/principals
 * configured in `ozone.administrator` (along with the user that starts OM,
 * which automatically becomes an Ozone administrator but not necessarily in
 * the config) are allowed to access this endpoint.
 *
 * If Kerberos is enabled, the principal should be appended to
 * `ozone.administrator`, e.g. `scm/scm@EXAMPLE.COM`
 * If Kerberos is not enabled, simply append the login user name to
 * `ozone.administrator`, e.g. `scm`
 */
public class OMDBCheckpointServlet extends DBCheckpointServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;

  @Override
  public void init() throws ServletException {

    OzoneManager om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);

    if (om == null) {
      LOG.error("Unable to initialize OMDBCheckpointServlet. OM is null");
      return;
    }

    OzoneConfiguration conf = om.getConfiguration();
    // Only Ozone Admins and Recon are allowed
    Collection<String> allowedUsers =
            new LinkedHashSet<>(om.getOmAdminUsernames());
    Collection<String> allowedGroups = om.getOmAdminGroups();
    ReconConfig reconConfig = conf.getObject(ReconConfig.class);
    String reconPrincipal = reconConfig.getKerberosPrincipal();
    if (!reconPrincipal.isEmpty()) {
      UserGroupInformation ugi =
          UserGroupInformation.createRemoteUser(reconPrincipal);
      allowedUsers.add(ugi.getShortUserName());
    }

    initialize(om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        allowedUsers,
        allowedGroups,
        om.isSpnegoEnabled());
  }
}
