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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.TestGetDBUpdatesAuthorization.RECON_PRINCIPAL;
import static org.apache.hadoop.ozone.om.TestGetDBUpdatesAuthorization.authTestConfiguration;
import static org.apache.hadoop.ozone.om.TestGetDBUpdatesAuthorization.getDBUpdates;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Tests getDBUpdates authorization when the Recon service principal is not configured.
 */
class TestGetDBUpdatesAuthorizationWithoutRecon {

  @Test
  void accessDeniedWhenReconPrincipalNotConfigured() throws Exception {
    OzoneConfiguration conf = authTestConfiguration();
    conf.unset(OZONE_RECON_KERBEROS_PRINCIPAL_KEY);
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    try {
      cluster.waitForClusterToBeReady();
      UserGroupInformation reconLikeUser =
          UserGroupInformation.createUserForTesting(RECON_PRINCIPAL, new String[] {"recon"});
      OMException ex = assertThrows(OMException.class, () -> getDBUpdates(conf, reconLikeUser));
      assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
    } finally {
      cluster.shutdown();
    }
  }
}
