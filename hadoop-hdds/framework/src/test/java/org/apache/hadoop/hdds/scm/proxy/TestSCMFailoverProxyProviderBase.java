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

package org.apache.hadoop.hdds.scm.proxy;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

/** Tests failover target selection. */
class TestSCMFailoverProxyProviderBase {
  private static final String SCM_SERVICE_ID = "scmservice";
  private static final String SCM_NODE_1 = "scm1";
  private static final String SCM_NODE_2 = "scm2";
  private static final String SCM_NODE_3 = "scm3";

  @Test
  void roundRobinContinuesAfterSuggestedLeaderFails() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_SCM_SERVICE_IDS_KEY, SCM_SERVICE_ID);
    configuration.set(OZONE_SCM_NODES_KEY + "." + SCM_SERVICE_ID,
        String.join(",", SCM_NODE_1, SCM_NODE_2, SCM_NODE_3));
    setAddress(configuration, SCM_NODE_1, 9861);
    setAddress(configuration, SCM_NODE_2, 9862);
    setAddress(configuration, SCM_NODE_3, 9863);
    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(configuration);

    provider.performFailoverToAssignedLeader(null,
        new ServerNotLeaderException(RaftPeerId.valueOf(SCM_NODE_1),
            "localhost:9862", "localhost", "SCM"));
    provider.performFailover(null);
    assertEquals(SCM_NODE_2, provider.getCurrentProxySCMNodeId());

    provider.performFailoverToAssignedLeader(null,
        new IOException("leader unavailable"));
    provider.performFailover(null);
    assertEquals(SCM_NODE_3, provider.getCurrentProxySCMNodeId());
  }

  private static void setAddress(OzoneConfiguration configuration,
      String nodeId, int port) {
    configuration.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
        SCM_SERVICE_ID, nodeId), "localhost");
    configuration.set(ConfUtils.addKeySuffixes(
        OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, SCM_SERVICE_ID, nodeId),
        "localhost:" + port);
  }
}
