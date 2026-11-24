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

package org.apache.hadoop.ozone.om.ha;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODE_ID_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OmConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMHANodeDetails}.
 */
class TestOMHANodeDetails {

  private static final String SERVICE_ID = "omservice";

  @Test
  void testNodeSpecificBindHost() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SERVICE_IDS_KEY, SERVICE_ID);
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, SERVICE_ID), "om1,om2,om3");
    conf.set(OZONE_OM_NODE_ID_KEY, "om1");
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, SERVICE_ID, "om1"), "localhost");
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, SERVICE_ID, "om2"), "[2001:db8::2]");
    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, SERVICE_ID, "om3"), "[2001:db8::3]");
    conf.set(ConfUtils.addKeySuffixes(OmConfig.Keys.BIND_HOST, SERVICE_ID, "om1"), "::");

    OMHANodeDetails.loadOMHAConfig(conf);

    InetSocketAddress rpcAddress = OmUtils.getOmAddress(conf);
    assertEquals("localhost", rpcAddress.getHostString());
    assertEquals(new InetSocketAddress(InetAddress.getByName("::"), OZONE_OM_PORT_DEFAULT),
        conf.getObject(OmConfig.class).getRpcBindAddress(rpcAddress));
  }
}
