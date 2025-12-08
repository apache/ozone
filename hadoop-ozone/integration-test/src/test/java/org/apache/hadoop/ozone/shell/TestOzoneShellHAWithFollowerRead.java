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

package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class tests Ozone sh shell command with FollowerRead.
 * Inspired by TestS3Shell
 */
public class TestOzoneShellHAWithFollowerRead extends TestOzoneShellHA {

  @BeforeAll
  @Override
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    OzoneManagerRatisServerConfig omHAConfig =
        conf.getObject(OzoneManagerRatisServerConfig.class);
    omHAConfig.setReadOption(RaftServerConfigKeys.Read.Option.LINEARIZABLE.name());

    conf.setFromObject(omHAConfig);
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean("ozone.om.ha.raft.server.read.leader.lease.enabled", true);
    conf.setBoolean("ozone.om.allow.leader.skip.linearizable.read", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    startKMS();
    startCluster(conf);
  }

  @Test
  public void testAllowLeaderSkipLinearizableRead() throws Exception {
    super.testListAllKeysInternal("skipvol1");
    long lastMetrics = getCluster().getOMLeader().getMetrics().getNumLeaderSkipLinearizableRead();
    Assertions.assertTrue(lastMetrics > 0);

    OzoneConfiguration oldConf = getCluster().getConf();
    OzoneConfiguration newConf = new OzoneConfiguration(oldConf);
    newConf.setBoolean("ozone.om.allow.leader.skip.linearizable.read", false);
    getCluster().getOMLeader().setConfiguration(newConf);

    super.testListAllKeysInternal("skipvol2");

    long curMetrics = getCluster().getOMLeader().getMetrics().getNumLeaderSkipLinearizableRead();
    Assertions.assertEquals(lastMetrics, curMetrics);

    getCluster().getOMLeader().setConfiguration(oldConf);
  }
}
