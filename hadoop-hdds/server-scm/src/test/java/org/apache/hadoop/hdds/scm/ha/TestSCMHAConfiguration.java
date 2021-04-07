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
package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY;

public class TestSCMHAConfiguration {
  private OzoneConfiguration conf;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
  }

  @Test
  public void testSetStorageDir() {
    SCMHAConfiguration scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    scmhaConfiguration.setRatisStorageDir("scm-ratis");
    conf.setFromObject(scmhaConfiguration);

    scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    Assert.assertEquals("scm-ratis", scmhaConfiguration.getRatisStorageDir());
  }

  @Test
  public void testRaftLogPurgeEnabled() {
    SCMHAConfiguration scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    scmhaConfiguration.setRaftLogPurgeEnabled(true);
    conf.setFromObject(scmhaConfiguration);

    scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    Assert.assertEquals(true, scmhaConfiguration.getRaftLogPurgeEnabled());
  }


  @Test
  public void testSCMHAConfig() throws Exception {
    String scmServiceId = "scmserviceId";
    conf.set(ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);

    String[] nodes = new String[] {"scm1", "scm2", "scm3"};
    conf.set(ScmConfigKeys.OZONE_SCM_NODES_KEY+"."+scmServiceId,
        "scm1,scm2,scm3");
    conf.set(ScmConfigKeys.OZONE_SCM_NODE_ID_KEY, "scm1");

    int port = 9880;
    int i = 1;
    for (String nodeId : nodes) {
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:"+port++);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
          scmServiceId, nodeId), port);
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY,
          scmServiceId, nodeId), "172.28.9.1");

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:"+port++);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
          scmServiceId, nodeId), port);
      conf.set(ConfUtils.addKeySuffixes(
          OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY, scmServiceId, nodeId),
          "172.28.9.1");

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:"+port++);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_PORT_KEY,
          scmServiceId, nodeId), port);
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_BIND_HOST_KEY,
          scmServiceId, nodeId), "172.28.9.1");

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:"+port++);
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_PORT_KEY,
          scmServiceId, nodeId), port);
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_BIND_HOST_KEY,
          scmServiceId, nodeId), "172.28.9.1");

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_HTTP_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost:"+port++);
      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_HTTP_BIND_HOST_KEY,
          scmServiceId, nodeId), "172.28.9.1");

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_DB_DIRS,
          scmServiceId, nodeId), "/var/scm-metadata"+ i++);

      conf.set(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
          scmServiceId, nodeId), "localhost");

      conf.setInt(ConfUtils.addKeySuffixes(OZONE_SCM_RATIS_PORT_KEY,
          scmServiceId, nodeId), port++);
    }


    SCMHANodeDetails.loadSCMHAConfig(conf);

    port = 9880;

    // Validate configs.
    Assert.assertEquals("localhost:"+port++,
        conf.get(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        scmServiceId, "scm1")));
    Assert.assertEquals(port,
        conf.getInt(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
        scmServiceId, "scm1"), 9999));
    Assert.assertEquals("172.28.9.1",
        conf.get(ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY,
            scmServiceId, "scm1")));


    Assert.assertEquals("localhost:"+port++,
        conf.get(ConfUtils.addKeySuffixes(
            OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY, scmServiceId, "scm1")));
    Assert.assertEquals(port, conf.getInt(ConfUtils.addKeySuffixes(
        OZONE_SCM_SECURITY_SERVICE_PORT_KEY, scmServiceId, "scm1"), 9999));
    Assert.assertEquals("172.28.9.1",
        conf.get(ConfUtils.addKeySuffixes(
            OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY, scmServiceId, "scm1")));


    Assert.assertEquals("localhost:"+port++,
        conf.get(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_ADDRESS_KEY,
            scmServiceId, "scm1")));
    Assert.assertEquals(port,
        conf.getInt(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_PORT_KEY,
            scmServiceId, "scm1"), 9999));
    Assert.assertEquals("172.28.9.1", conf.get(
        ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_BIND_HOST_KEY, scmServiceId,
        "scm1")));

    Assert.assertEquals("localhost:"+port++,
        conf.get(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_ADDRESS_KEY,
            scmServiceId, "scm1")));
    Assert.assertEquals(port,
        conf.getInt(ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_PORT_KEY,
            scmServiceId, "scm1"), 9999));
    Assert.assertEquals("172.28.9.1", conf.get(
        ConfUtils.addKeySuffixes(OZONE_SCM_DATANODE_BIND_HOST_KEY, scmServiceId,
        "scm1")));


    Assert.assertEquals("localhost:"+port++,
        conf.get(ConfUtils.addKeySuffixes(OZONE_SCM_HTTP_ADDRESS_KEY,
        scmServiceId, "scm1")));
    Assert.assertEquals("172.28.9.1",
        conf.get(ConfUtils.addKeySuffixes(OZONE_SCM_HTTP_BIND_HOST_KEY,
        scmServiceId, "scm1")));

    Assert.assertEquals("localhost", conf.get(ConfUtils.addKeySuffixes(
        OZONE_SCM_ADDRESS_KEY, scmServiceId,
        "scm1")));

    Assert.assertEquals("/var/scm-metadata1",
        conf.get(ConfUtils.addKeySuffixes(OZONE_SCM_DB_DIRS, scmServiceId,
        "scm1")));

    Assert.assertEquals(port++,
        conf.getInt(ConfUtils.addKeySuffixes(OZONE_SCM_RATIS_PORT_KEY,
        scmServiceId, "scm1"), 9999));


  }
}
