/*
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

package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY;

/**
 * Tests for {@link SCMNodeInfo}.
 */
public class TestSCMNodeInfo {

  private OzoneConfiguration conf = new OzoneConfiguration();
  private String scmServiceId = "scmserviceId";
  private String[] nodes = new String[]{"scm1", "scm2", "scm3"};

  @Before
  public void setup() {
    conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
    conf.set(ScmConfigKeys.OZONE_SCM_NODES_KEY + "." + scmServiceId,
        "scm1,scm2,scm3");
  }

  @Test
  public void testScmHANodeInfo() {
    int port = 9880;
    for (String nodeId : nodes) {
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost");
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:" + ++port);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
          scmServiceId, nodeId), port);

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:" + ++port);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
          scmServiceId, nodeId), port);

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:" + ++port);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_PORT_KEY,
          scmServiceId, nodeId), port);

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:" + ++port);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_PORT_KEY,
          scmServiceId, nodeId), port);

    }

    List<SCMNodeInfo> scmNodeInfos = SCMNodeInfo.buildNodeInfo(conf);

    port = 9880;

    int count = 1;
    for (SCMNodeInfo scmNodeInfo : scmNodeInfos) {
      Assert.assertEquals(scmServiceId, scmNodeInfo.getServiceId());
      Assert.assertEquals("scm"+count++, scmNodeInfo.getNodeId());
      Assert.assertEquals("localhost:" + ++port,
          scmNodeInfo.getBlockClientAddress());
      Assert.assertEquals("localhost:" + ++port,
          scmNodeInfo.getScmSecurityAddress());
      Assert.assertEquals("localhost:" + ++port,
          scmNodeInfo.getScmClientAddress());
      Assert.assertEquals("localhost:" + ++port,
          scmNodeInfo.getScmDatanodeAddress());
    }
  }

  @Test
  public void testSCMHANodeInfoWithDefaultPorts() {
    for (String nodeId : nodes) {
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost");
    }

    List<SCMNodeInfo> scmNodeInfos = SCMNodeInfo.buildNodeInfo(conf);

    int count = 1;
    for (SCMNodeInfo scmNodeInfo : scmNodeInfos) {
      Assert.assertEquals(scmServiceId, scmNodeInfo.getServiceId());
      Assert.assertEquals("scm"+count++, scmNodeInfo.getNodeId());
      Assert.assertEquals("localhost:" + OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT,
          scmNodeInfo.getBlockClientAddress());
      Assert.assertEquals("localhost:" +
              OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT,
          scmNodeInfo.getScmSecurityAddress());
      Assert.assertEquals("localhost:" + OZONE_SCM_CLIENT_PORT_DEFAULT,
          scmNodeInfo.getScmClientAddress());
      Assert.assertEquals("localhost:" + OZONE_SCM_DATANODE_PORT_DEFAULT,
          scmNodeInfo.getScmDatanodeAddress());
    }


  }

  @Test(expected = ConfigurationException.class)
  public void testSCMHANodeInfoWithMissingSCMAddress() {
    conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
        scmServiceId, "scm1"), "localhost");
    conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
        scmServiceId, "scm1"), "localhost");

    SCMNodeInfo.buildNodeInfo(conf);
  }

  @Test
  public void testNonHAWithRestDefaults() {
    OzoneConfiguration config = new OzoneConfiguration();

    config.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");

    List< SCMNodeInfo > scmNodeInfos = SCMNodeInfo.buildNodeInfo(config);

    Assert.assertNotNull(scmNodeInfos);
    Assert.assertTrue(scmNodeInfos.size() == 1);
    Assert.assertEquals("localhost:" + OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT,
        scmNodeInfos.get(0).getBlockClientAddress());
    Assert.assertEquals("localhost:" + OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT,
        scmNodeInfos.get(0).getScmSecurityAddress());
    Assert.assertEquals("localhost:" + OZONE_SCM_CLIENT_PORT_DEFAULT,
        scmNodeInfos.get(0).getScmClientAddress());
    Assert.assertEquals("localhost:" + OZONE_SCM_DATANODE_PORT_DEFAULT,
        scmNodeInfos.get(0).getScmDatanodeAddress());
  }


}
