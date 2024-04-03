/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 * This class is to test the serialization/deserialization of cluster tree
 * information from SCM.
 */
@Timeout(300)
public class TestGetClusterTreeInformation {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestGetClusterTreeInformation.class);
  private static int numOfDatanodes = 3;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;

  @BeforeAll
  public static void init() throws IOException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setNumOfOzoneManagers(3)
        .setNumOfStorageContainerManagers(3)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetClusterTreeInformation() throws IOException {
    SCMBlockLocationFailoverProxyProvider failoverProxyProvider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    failoverProxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            failoverProxyProvider);

    InnerNode expectedInnerNode = (InnerNode) scm.getClusterMap().getNode(ROOT);
    InnerNode actualInnerNode = scmBlockLocationClient.getNetworkTopology();
    assertEquals(expectedInnerNode, actualInnerNode);
  }
}
