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
package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmRequestProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.ozone.test.GenericTestUtils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import java.io.File;

/**
 * Unit tests to validate the SCMClientProtocolServer
 * servicing commands from the scm client.
 */
public class TestSCMClientProtocolServer {
  private OzoneConfiguration config;
  private SCMClientProtocolServer server;
  private StorageContainerManager scm;
  private StorageContainerLocationProtocolServerSideTranslatorPB service;

  @BeforeEach
  public void setUp() throws Exception {
    config = new OzoneConfiguration();
    File dir = GenericTestUtils.getRandomizedTestDir();
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    scm = HddsTestUtils.getScm(config, configurator);
    scm.start();
    scm.exitSafeMode();

    server = scm.getClientProtocolServer();
    service = new StorageContainerLocationProtocolServerSideTranslatorPB(server,
        scm, Mockito.mock(ProtocolMessageMetrics.class));
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
  }

  /**
   * Tests decommissioning of scm.
   */
  @Test
  public void testScmDecommissionRemoveScmErrors() throws Exception {
    String scmId = scm.getScmId();
    String err = "Cannot remove current leader.";

    DecommissionScmRequestProto request =
        DecommissionScmRequestProto.newBuilder()
            .setScmId(scmId)
            .build();

    DecommissionScmResponseProto resp =
        service.decommissionScm(request);

    // should have optional error message set in response
    assertTrue(resp.hasErrorMsg());
    assertTrue(resp.getErrorMsg()
        .equals(err));
  }
}
