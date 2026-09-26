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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.scm.proxy.SCMFailoverProxyProviderBase;
import org.apache.hadoop.hdds.scm.proxy.SCMProxyInfo;
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
 * Tests the OM's SCM nodes reconfiguration wiring: the SCM node list and the
 * per-node SCM addresses must be reconfigurable on a running OM, reloading its
 * SCM failover proxies without a restart. Proxy-level add/remove behavior is
 * covered by {@link org.apache.hadoop.hdds.scm.proxy.SCMFailoverProxyProviderBase}
 * unit tests; this verifies the OM-side registration and callbacks end to end.
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
   * Ensures SCM node list and address prefix configurations are reconfigurable on the OM.
   */
  @Test
  void testScmNodesAndAddressReconfigurableOnOm() throws Exception {
    ReconfigurationHandler handler =
        cluster.getOzoneManager().getReconfigurationHandler();
    String scmNodesKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);

    assertTrue(handler.isPropertyReconfigurable(scmNodesKey));
    assertTrue(handler.listReconfigureProperties().contains(scmNodesKey));

    // Register address prefix so dynamically added nodes can be reconfigured.
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      String scmAddrKey = ConfUtils.addKeySuffixes(
          OZONE_SCM_ADDRESS_KEY, scmServiceId, scm.getSCMNodeId());
      assertTrue(handler.isPropertyReconfigurable(scmAddrKey));
    }
  }

  /**
   * Verifies that an empty SCM node list is rejected without modifying existing proxies.
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
   * Verifies that reconfiguring SCM nodes dynamically updates block and container failover proxies.
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

    // Remove one SCM; its lingering address configuration allows reloading the remaining nodes.
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

    // Perform live RPCs to verify rebuilt proxies rather than just checking node IDs.
    assertNotNull(scmClient.getContainerClient().getScmInfo());
    assertNotNull(scmClient.getBlockClient().getScmInfo());
  }

  /**
   * Verifies that updating a per-node SCM address reloads failover proxies via the completion callback.
   */
  @Test
  void testReconfigureScmAddressReloadsProxies() throws Exception {
    OzoneManager om = cluster.getOzoneManager();
    ScmClient scmClient = om.getScmClient();
    String nodeId =
        cluster.getStorageContainerManagers().get(0).getSCMNodeId();
    String scmAddrKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_ADDRESS_KEY, scmServiceId, nodeId);

    // Update SCM address in OM's live configuration read by proxy providers.
    OzoneConfiguration conf = om.getConfiguration();
    conf.set(scmAddrKey, "127.0.0.2");

    Map<String, Boolean> changed = new HashMap<>();
    changed.put(scmAddrKey, true);
    om.reloadScmProxiesOnReconfig(changed, conf);

    // Both providers must have rebuilt that node's proxy info to the new host.
    assertResolvedHost(scmClient.getContainerProxyProvider(), nodeId,
        "127.0.0.2");
    assertResolvedHost(scmClient.getBlockProxyProvider(), nodeId, "127.0.0.2");
  }

  /**
   * Verifies that proxies are reloaded only when reconfiguration touches SCM node lists or addresses.
   */
  @Test
  void testReloadScmProxiesOnReconfigIgnoresUnrelatedKey() {
    OzoneManager om = cluster.getOzoneManager();
    ScmClient scmClient = om.getScmClient();
    String nodeId =
        cluster.getStorageContainerManagers().get(0).getSCMNodeId();
    String scmAddrKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_ADDRESS_KEY, scmServiceId, nodeId);

    OzoneConfiguration conf = om.getConfiguration();
    String original =
        resolvedHost(scmClient.getContainerProxyProvider(), nodeId);

    // Drift the live address, but report only an unrelated key as changed.
    conf.set(scmAddrKey, "127.0.0.9");
    Map<String, Boolean> changed = new HashMap<>();
    changed.put("ozone.om.unrelated.key", true);
    om.reloadScmProxiesOnReconfig(changed, conf);

    // No reload fired, so both providers still resolve the original address.
    assertEquals(original,
        resolvedHost(scmClient.getContainerProxyProvider(), nodeId));
    assertEquals(original,
        resolvedHost(scmClient.getBlockProxyProvider(), nodeId));
  }

  /**
   * Verifies that malformed SCM addresses during completion callbacks are caught
   * and logged without breaking existing proxies.
   */
  @Test
  void testReloadScmProxiesOnReconfigCatchesMalformedAddress() {
    OzoneManager om = cluster.getOzoneManager();
    ScmClient scmClient = om.getScmClient();
    String nodeId =
        cluster.getStorageContainerManagers().get(0).getSCMNodeId();
    String scmAddrKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_ADDRESS_KEY, scmServiceId, nodeId);

    OzoneConfiguration conf = om.getConfiguration();
    Set<String> before =
        new HashSet<>(scmClient.getContainerProxyProvider().getSCMNodeIds());

    // Addresses with duplicate ports produce invalid host:port authorities and throw IllegalArgumentException.
    conf.set(scmAddrKey, "127.0.0.1:9999");
    Map<String, Boolean> changed = new HashMap<>();
    changed.put(scmAddrKey, true);

    // The callback must swallow the failure rather than break the chain.
    assertDoesNotThrow(() -> om.reloadScmProxiesOnReconfig(changed, conf));

    // The membership is left in place for both providers.
    assertEquals(before,
        new HashSet<>(scmClient.getContainerProxyProvider().getSCMNodeIds()));
    assertEquals(before,
        new HashSet<>(scmClient.getBlockProxyProvider().getSCMNodeIds()));
  }

  /**
   * Verifies that adding an SCM without a resolved address fails reconfiguration and keeps proxies unchanged.
   */
  @Test
  void testReconfigureScmNodesFailsWhenAddressMissing() {
    OzoneManager om = cluster.getOzoneManager();
    ReconfigurationHandler handler = om.getReconfigurationHandler();
    ScmClient scmClient = om.getScmClient();
    OzoneConfiguration conf = om.getConfiguration();
    String scmNodesKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);

    List<String> before =
        new ArrayList<>(scmClient.getContainerProxyProvider().getSCMNodeIds());
    String originalValue = conf.get(scmNodesKey);

    List<String> withMissing = new ArrayList<>(before);
    withMissing.add("scm-no-address");

    assertThrows(ReconfigurationException.class, () ->
        handler.reconfigureProperty(scmNodesKey, String.join(",", withMissing)));

    // The node list is rolled back and both providers keep their membership.
    assertEquals(originalValue, conf.get(scmNodesKey));
    assertEquals(new HashSet<>(before),
        new HashSet<>(scmClient.getContainerProxyProvider().getSCMNodeIds()));
    assertEquals(new HashSet<>(before),
        new HashSet<>(scmClient.getBlockProxyProvider().getSCMNodeIds()));
  }

  /**
   * Verifies that adding an SCM with a malformed address rolls back node changes and leaves proxies unchanged.
   */
  @Test
  void testReconfigureScmNodesFailsWhenAddressMalformed() {
    OzoneManager om = cluster.getOzoneManager();
    ReconfigurationHandler handler = om.getReconfigurationHandler();
    ScmClient scmClient = om.getScmClient();
    OzoneConfiguration conf = om.getConfiguration();
    String scmNodesKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);
    String newNodeId = "scm-bad-address";
    String newAddrKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY, scmServiceId, newNodeId);

    List<String> before =
        new ArrayList<>(scmClient.getContainerProxyProvider().getSCMNodeIds());
    String originalValue = conf.get(scmNodesKey);

    // Addresses with existing ports form invalid authorities,
    // throwing IllegalArgumentException over ConfigurationException.
    conf.set(newAddrKey, "127.0.0.1:9999");
    List<String> withBadAddress = new ArrayList<>(before);
    withBadAddress.add(newNodeId);

    assertThrows(ReconfigurationException.class, () -> handler
        .reconfigureProperty(scmNodesKey, String.join(",", withBadAddress)));

    // The node list is rolled back and both providers keep their membership.
    assertEquals(originalValue, conf.get(scmNodesKey));
    assertEquals(new HashSet<>(before),
        new HashSet<>(scmClient.getContainerProxyProvider().getSCMNodeIds()));
    assertEquals(new HashSet<>(before),
        new HashSet<>(scmClient.getBlockProxyProvider().getSCMNodeIds()));
  }

  /**
   * Verifies that adding an SCM before setting its address rolls back initially,
   * then succeeds when retried after setting the address.
   */
  @Test
  void testReconfigureAddScmNodeNodesBeforeAddress() throws Exception {
    OzoneManager om = cluster.getOzoneManager();
    ReconfigurationHandler handler = om.getReconfigurationHandler();
    ScmClient scmClient = om.getScmClient();
    String scmNodesKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);

    List<String> before =
        new ArrayList<>(scmClient.getContainerProxyProvider().getSCMNodeIds());
    String newNodeId = "scm-added";
    String newAddrKey =
        ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY, scmServiceId, newNodeId);
    List<String> after = new ArrayList<>(before);
    after.add(newNodeId);
    String requested = String.join(",", after);

    // 1. Unresolvable node list fails and rolls back when applied before the new address.
    assertThrows(ReconfigurationException.class,
        () -> handler.reconfigureProperty(scmNodesKey, requested));
    assertFalse(new HashSet<>(
        scmClient.getContainerProxyProvider().getSCMNodeIds()).contains(newNodeId));

    // 2. The new SCM's address is applied (prefix key, no reload of its own).
    handler.reconfigureProperty(newAddrKey, "127.0.0.1");

    // 3. Reapplying the node list now resolves the new SCM, so it is added.
    handler.reconfigureProperty(scmNodesKey, requested);
    Set<String> expected = new HashSet<>(after);
    assertEquals(expected,
        new HashSet<>(scmClient.getContainerProxyProvider().getSCMNodeIds()));
    assertEquals(expected,
        new HashSet<>(scmClient.getBlockProxyProvider().getSCMNodeIds()));
  }

  private static void assertResolvedHost(
      SCMFailoverProxyProviderBase<?> provider,
      String nodeId, String expectedHost) {
    String host = resolvedHost(provider, nodeId);
    assertNotNull(host);
    assertEquals(expectedHost, host);
  }

  private static String resolvedHost(
      SCMFailoverProxyProviderBase<?> provider, String nodeId) {
    for (SCMProxyInfo candidate : provider.getSCMProxyInfoList()) {
      if (candidate.getNodeId().equals(nodeId)) {
        return candidate.getAddress().getAddress().getHostAddress();
      }
    }
    return null;
  }
}
