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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test the OM's SCM nodes reconfiguration wiring: the SCM node list and the
 * per-node SCM addresses must be reconfigurable on a running OM so that the OM
 * can reload its SCM failover proxies without a restart. The proxy-level
 * add/remove behavior is covered by
 * {@link org.apache.hadoop.hdds.scm.proxy.SCMFailoverProxyProviderBase}'s unit
 * tests; this verifies the OM-side registration and callback end to end.
 */
@Timeout(300)
public class TestOmSCMNodesReconfiguration {

  private MiniOzoneHAClusterImpl cluster = null;
  private String scmServiceId;

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    scmServiceId = "scm-service-test1";
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test1")
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * The SCM node list and each SCM's address (registered as a prefix) must be
   * reconfigurable on the OM.
   */
  @Test
  void testScmNodesAndAddressReconfigurableOnOm() throws Exception {
    ReconfigurationHandler handler =
        cluster.getOzoneManager().getReconfigurationHandler();
    String scmNodesKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);

    assertTrue(handler.isPropertyReconfigurable(scmNodesKey));
    assertTrue(handler.listReconfigureProperties().contains(scmNodesKey));

    // The per-node SCM address keys are registered as a prefix, so any node's
    // address key is reconfigurable even though it was not registered by name.
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      String scmAddrKey = ConfUtils.addKeySuffixes(
          OZONE_SCM_ADDRESS_KEY, scmServiceId, scm.getSCMNodeId());
      assertTrue(handler.isPropertyReconfigurable(scmAddrKey));
    }
  }

  /**
   * Setting an empty SCM node list must be rejected, leaving the OM's SCM
   * proxies untouched.
   */
  @Test
  void testReconfigureScmNodesToBlankThrows() {
    ReconfigurationHandler handler =
        cluster.getOzoneManager().getReconfigurationHandler();
    String scmNodesKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);

    assertThrows(ReconfigurationException.class,
        () -> handler.reconfigureProperty(scmNodesKey, ""));
  }

  /**
   * Reconfiguring the SCM node list on a running OM must reload the SCM failover
   * proxies to the new membership. Dropping one SCM from the list has to shrink
   * the proxy node set for both the block and container providers; the reload
   * reads the list from the (freshly written) configuration, so reconfiguring to
   * a genuinely different value is what exercises the wiring.
   */
  @Test
  void testReconfigureScmNodesReloadsProxies() throws Exception {
    OzoneManager om = cluster.getOzoneManager();
    ReconfigurationHandler handler = om.getReconfigurationHandler();
    ScmClient scmClient = om.getScmClient();
    String scmNodesKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);

    List<String> before =
        new ArrayList<>(scmClient.getContainerProxyProvider().getSCMNodeIds());
    assertEquals(3, before.size());

    // Drop one SCM from the OM's view. Its address stays in the configuration,
    // so the reload of the remaining nodes succeeds.
    String dropped = before.get(before.size() - 1);
    List<String> remaining = new ArrayList<>(before.subList(0, before.size() - 1));
    Set<String> expected = new HashSet<>(remaining);

    handler.reconfigureProperty(scmNodesKey, String.join(",", remaining));

    Set<String> afterContainer =
        new HashSet<>(scmClient.getContainerProxyProvider().getSCMNodeIds());
    Set<String> afterBlock =
        new HashSet<>(scmClient.getBlockProxyProvider().getSCMNodeIds());
    assertEquals(expected, afterContainer);
    assertEquals(expected, afterBlock);
    assertFalse(afterContainer.contains(dropped));
  }
}
