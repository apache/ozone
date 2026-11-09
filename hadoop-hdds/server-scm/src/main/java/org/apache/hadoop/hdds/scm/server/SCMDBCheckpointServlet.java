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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_HTTP_AUTH_TYPE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import javax.servlet.ServletException;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the current checkpoint Snapshot of the SCM DB. (tar.gz)
 *
 * When Ozone authorization is enabled (`ozone.authorization.enabled`=`true`),
 * only users/principals configured in `ozone.administrator` (along with the
 * user that starts SCM, which automatically becomes an Ozone administrator
 * but not necessarily in the config) are allowed to access this endpoint.
 *
 * If Kerberos is enabled, the principal should be appended to
 * `ozone.administrator`, e.g. `scm/scm@EXAMPLE.COM`
 * If Kerberos is not enabled, simply append the login user name to
 * `ozone.administrator`, e.g. `scm`
 *
 * Recon (`ozone.recon.kerberos.principal`) is also allowed, to download the SCM DB.
 */
public class SCMDBCheckpointServlet extends DBCheckpointServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;
  private transient StorageContainerManager scm;

  @Override
  public void init() throws ServletException {

    scm = (StorageContainerManager) getServletContext()
        .getAttribute(OzoneConsts.SCM_CONTEXT_ATTRIBUTE);

    if (scm == null) {
      LOG.error("Unable to initialize SCMDBCheckpointServlet. SCM is null");
      return;
    }

    // Only SCM admins and Recon are allowed
    Collection<String> allowedUsers = new LinkedHashSet<>(scm.getScmAdminUsernames());
    String reconPrincipal = scm.getConfiguration().getObject(ReconConfig.class).getKerberosPrincipal();
    if (!reconPrincipal.isEmpty()) {
      allowedUsers.add(UserGroupInformation.createRemoteUser(reconPrincipal).getShortUserName());
    }

    initialize(scm.getScmMetadataStore().getStore(),
        scm.getMetrics().getDBCheckpointMetrics(),
        scm.isAdminAuthorizationEnabled(),
        allowedUsers,
        scm.getScmAdminGroups(),
        "kerberos".equals(scm.getConfiguration().get(HDDS_SCM_HTTP_AUTH_TYPE, "simple")));
  }

  /**
   * Like the inter-SCM gRPC download, flushes the SCM transaction buffer and looks up the store per request,
   * because installing a checkpoint replaces it.
   */
  @Override
  public DBCheckpoint getCheckpoint(Path ignoredTmpdir, boolean flush) throws IOException {
    // Do not flush a partially applied transaction into the checkpoint (HDDS-16452).
    scm.getScmHAManager().asSCMHADBTransactionBuffer().flushIfNeeded(0);
    return scm.getScmMetadataStore().getStore().getCheckpoint(flush);
  }
}
