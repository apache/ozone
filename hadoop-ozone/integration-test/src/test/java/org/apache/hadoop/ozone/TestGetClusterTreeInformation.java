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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.ozone.test.HATests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This class is to test the serialization/deserialization of cluster tree
 * information from SCM.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestGetClusterTreeInformation implements HATests.TestCase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestGetClusterTreeInformation.class);
  private OzoneConfiguration conf;
  private StorageContainerManager scm;

  @BeforeAll
  void init() {
    conf = cluster().getConf();
    scm = cluster().getStorageContainerManager();
  }

  @Test
  public void testGetClusterTreeInformation() throws IOException {
    SCMBlockLocationFailoverProxyProvider failoverProxyProvider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    failoverProxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            failoverProxyProvider, conf);

    InnerNode expectedInnerNode = (InnerNode) scm.getClusterMap().getNode(ROOT);
    InnerNode actualInnerNode = scmBlockLocationClient.getNetworkTopology();
    assertEquals(expectedInnerNode, actualInnerNode);
  }
}
