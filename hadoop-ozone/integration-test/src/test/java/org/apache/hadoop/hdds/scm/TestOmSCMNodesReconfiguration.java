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

    // Comparing node ids alone would not catch a proxy that was stopped but
    // left in use, so drive a live RPC through the rebuilt proxies.
    assertNotNull(scmClient.getContainerClient().getScmInfo());
    assertNotNull(scmClient.getBlockClient().getScmInfo());
  }

  /**
   * Changing only a per-node SCM address (no node-list change) must reload the
   * OM's SCM failover proxies against the new endpoint. The address keys are
   * registered as a prefix with no per-key reload function, so an address-only
   * change is applied by the reconfiguration-complete callback, not the
   * per-property path. Drive that callback directly: the async {@code reconfig
   * start} path reads ozone-site.xml from disk, which a mini-cluster does not
   * rewrite, so it cannot be exercised end to end here.
   */
  @Test
  void testReconfigureScmAddressReloadsProxies() throws Exception {
    OzoneManager om = cluster.getOzoneManager();
    ScmClient scmClient = om.getScmClient();
    String nodeId =
        cluster.getStorageContainerManagers().get(0).getSCMNodeId();
    String scmAddrKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_ADDRESS_KEY, scmServiceId, nodeId);

    // Point one SCM at a different resolvable address on the OM's live
    // configuration -- the same instance the proxy providers read.
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
   * The reconfiguration-complete callback must reload the SCM proxies only when
   * the batch touched the SCM node list or a per-node SCM address. A batch that
   * reports only an unrelated key must leave the proxies untouched, even if the
   * live SCM address configuration has drifted, so nothing is applied.
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
   * A malformed per-node SCM address in a batch reported to the complete callback
   * must not escape: the callback catches the resolution failure and only logs,
   * leaving the previous proxies in place, so the reconfiguration-complete chain
   * (tracing, logging) is not broken.
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

    // An address that already carries a port cannot be rebuilt into a valid
    // host:port authority, so resolving the membership throws
    // IllegalArgumentException.
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
   * Adding an SCM to the node list without first setting its address must fail
   * the reconfiguration (so it is reported FAILED and can be retried) and must
   * leave the live configuration and the SCM proxies unchanged -- the node list
   * must never keep an SCM without a resolvable address.
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
   * Adding an SCM whose address is malformed (an explicit host:port value, which
   * cannot be reassembled into a valid socket address) must fail the same way as a
   * missing address: resolving the membership throws IllegalArgumentException, so
   * the reload failure is caught, the node list is rolled back, and the SCM proxies
   * are left unchanged. This guards the widened catch that would otherwise let the
   * live configuration keep a node with an unusable address.
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

    // An address that already carries a port cannot be rebuilt into a valid
    // host:port authority, so resolving the new membership throws
    // IllegalArgumentException rather than ConfigurationException.
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
   * Adding an SCM, applying the keys in "nodes before address" order (the order
   * a real {@code reconfig start} batch may use). Applying the node list before
   * the new SCM's address cannot resolve the node, so that property fails and is
   * rolled back and the node is not added; once the address is set, reapplying
   * the node list adds it. This mirrors the datanode, which skips an SCM whose
   * address is not resolvable yet, so adding a node is retriable rather than
   * silently leaving a node without an address in the configuration.
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

    // 1. Node list applied first, before the new SCM's address: the reload
    //    cannot resolve the node, so the property fails and is rolled back.
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
