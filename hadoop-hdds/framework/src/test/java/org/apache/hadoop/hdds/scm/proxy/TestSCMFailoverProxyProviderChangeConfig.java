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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SCMFailoverProxyProviderBase#changeConfig()} reloads the
 * SCM node list from an updated configuration (the dynamic SCM reconfiguration
 * scenario), adding and removing nodes and keeping the current proxy pointer
 * valid, while leaving the previous state intact if the new configuration is
 * incomplete.
 */
public class TestSCMFailoverProxyProviderChangeConfig {

  private static final String SERVICE_ID = "scmservice";

  private static OzoneConfiguration haConf(String nodes) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_SERVICE_IDS_KEY, SERVICE_ID);
    conf.set(ConfUtils.addSuffix(OZONE_SCM_NODES_KEY, SERVICE_ID), nodes);
    return conf;
  }

  private static void setAddress(OzoneConfiguration conf, String nodeId,
      String host) {
    conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY, SERVICE_ID, nodeId),
        host);
  }

  @Test
  public void testChangeConfigAddsNode() {
    OzoneConfiguration conf = haConf("scm1,scm2");
    setAddress(conf, "scm1", "host1");
    setAddress(conf, "scm2", "host2");

    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    assertEquals(2, provider.getSCMNodeIds().size());

    // Operator adds a third SCM: its address key first, then the node list.
    setAddress(conf, "scm3", "host3");
    conf.set(ConfUtils.addSuffix(OZONE_SCM_NODES_KEY, SERVICE_ID),
        "scm1,scm2,scm3");
    provider.changeConfig();

    List<String> nodeIds = provider.getSCMNodeIds();
    assertEquals(3, nodeIds.size());
    assertTrue(nodeIds.contains("scm3"));
  }

  @Test
  public void testChangeConfigRemovesNode() {
    OzoneConfiguration conf = haConf("scm1,scm2,scm3");
    setAddress(conf, "scm1", "host1");
    setAddress(conf, "scm2", "host2");
    setAddress(conf, "scm3", "host3");

    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    // Point the current proxy at the node that is about to be removed.
    provider.changeCurrentProxy("scm3");

    conf.set(ConfUtils.addSuffix(OZONE_SCM_NODES_KEY, SERVICE_ID), "scm1,scm2");
    provider.changeConfig();

    List<String> nodeIds = provider.getSCMNodeIds();
    assertEquals(2, nodeIds.size());
    assertTrue(nodeIds.contains("scm1"));
    assertTrue(nodeIds.contains("scm2"));
    // The current proxy pointer must fall back to a still-configured node.
    assertTrue(nodeIds.contains(provider.getCurrentProxySCMNodeId()));
  }

  @Test
  public void testChangeConfigFailsWhenAddressMissing() {
    OzoneConfiguration conf = haConf("scm1,scm2");
    setAddress(conf, "scm1", "host1");
    setAddress(conf, "scm2", "host2");

    SCMBlockLocationFailoverProxyProvider provider =
        new SCMBlockLocationFailoverProxyProvider(conf);

    // Node list references scm3 but its address has not been set yet. This
    // mimics reconfiguring the node list before the new node's address; the
    // reload must fail and leave the previous node set intact for a retry.
    conf.set(ConfUtils.addSuffix(OZONE_SCM_NODES_KEY, SERVICE_ID),
        "scm1,scm2,scm3");
    assertThrows(ConfigurationException.class, provider::changeConfig);

    List<String> nodeIds = provider.getSCMNodeIds();
    assertEquals(2, nodeIds.size());
    assertTrue(nodeIds.contains("scm1"));
    assertTrue(nodeIds.contains("scm2"));
  }
}
