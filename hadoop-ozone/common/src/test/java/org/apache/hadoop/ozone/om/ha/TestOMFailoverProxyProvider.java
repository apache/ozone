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
package org.apache.hadoop.ozone.om.ha;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.
    OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.
    OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;

/**
 * Test OMFailoverProxyProvider failover behaviour.
 */
public class TestOMFailoverProxyProvider {
  private final static String omServiceId = "om-service-test1";
  private final static String nodeIdBaseStr = "omNode-";
  private final static String dummyNodeAddr = "0.0.0.0:8080";

  /**
   * 1. Create FailoverProvider with 3 OMs.
   * 2. Try failover to different node: WaitTime should be 0.
   * 3. Try failover to same node: wiatTime should increment
   * attempts*waitBetweenRetries
   */
  @Test
  public void testWaitTimeWithSameNodeFailover() throws IOException {
    String nodeId1 = nodeIdBaseStr + 1;
    String nodeId2 = nodeIdBaseStr + 2;
    String nodeId3 = nodeIdBaseStr + 3;
    OzoneConfiguration config = new OzoneConfiguration();
    //config.set(OZONE_OM_NODE_ID_KEY, nodeId);
    config.set(OmUtils.addKeySuffixes(OZONE_OM_NODES_KEY, omServiceId),
        String.join(",", nodeId1, nodeId2, nodeId3));
    config.set(OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, omServiceId,
        nodeId1), dummyNodeAddr);
    config.set(OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, omServiceId,
        nodeId2), dummyNodeAddr);
    config.set(OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, omServiceId,
        nodeId3), dummyNodeAddr);
    long waitBetweenRetries = config.getLong(
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
    OMFailoverProxyProvider provider = new OMFailoverProxyProvider(config,
        UserGroupInformation.getCurrentUser(), omServiceId);
    provider.performFailoverToNextProxy(); // Failover attempt 1
    Assert.assertEquals(0, provider.getWaitTime());
    provider.performFailoverToNextProxy(); // Failover attempt 2
    Assert.assertEquals(0, provider.getWaitTime());
    // Failover attempt 3 to same OM, waitTime should increase after this
    provider.performFailoverIfRequired(provider.getCurrentProxyOMNodeId());
    Assert.assertEquals(waitBetweenRetries, provider.getWaitTime());
    // Failover attempt 4 to same OM, waitTime should further increase after this
    provider.performFailoverIfRequired(provider.getCurrentProxyOMNodeId());
    Assert.assertEquals(waitBetweenRetries * 2, provider.getWaitTime());
    // Failover attempt 5 to same OM, waitTime should further increase after this
    provider.performFailoverIfRequired(provider.getCurrentProxyOMNodeId());
    Assert.assertEquals(waitBetweenRetries * 3, provider.getWaitTime());
    // Failover attempt 6 to same OM, waitTime should further increase after this
    provider.performFailoverIfRequired(provider.getCurrentProxyOMNodeId());
    Assert.assertEquals(waitBetweenRetries * 4, provider.getWaitTime());

    // Failover attempt 7, will go to new OM and waitTime should be 0
    provider.performFailoverToNextProxy();
    Assert.assertEquals(0, provider.getWaitTime());
  }
}
