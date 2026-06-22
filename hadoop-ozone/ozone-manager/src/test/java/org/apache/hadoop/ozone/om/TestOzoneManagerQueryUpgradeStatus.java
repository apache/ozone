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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.QueryUpgradeStatusResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link OzoneManager#queryUpgradeStatus()}.
 */
public class TestOzoneManagerQueryUpgradeStatus {

  private OzoneManager ozoneManager;
  private OMVersionManager versionManager;
  private StorageContainerLocationProtocol containerClient;

  @BeforeEach
  public void setup() {
    ozoneManager = Mockito.mock(OzoneManager.class, Mockito.CALLS_REAL_METHODS);

    ScmClient scmClient = mock(ScmClient.class);
    containerClient = mock(StorageContainerLocationProtocol.class);
    when(scmClient.getContainerClient()).thenReturn(containerClient);

    versionManager = mock(OMVersionManager.class);

    HddsWhiteboxTestUtils.setInternalState(ozoneManager, "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(ozoneManager, "versionManager", versionManager);
  }

  @Test
  public void testOmFinalizedWhenFinalizationNotNeeded() throws IOException {
    when(versionManager.needsFinalization()).thenReturn(false);
    when(containerClient.queryUpgradeStatus()).thenReturn(HddsProtos.UpgradeStatus.getDefaultInstance());

    QueryUpgradeStatusResponse response = ozoneManager.queryUpgradeStatus();

    assertTrue(response.getOmFinalized());
  }

  @Test
  public void testOmNotFinalizedWhenFinalizationNeeded() throws IOException {
    when(versionManager.needsFinalization()).thenReturn(true);
    when(containerClient.queryUpgradeStatus()).thenReturn(HddsProtos.UpgradeStatus.getDefaultInstance());

    QueryUpgradeStatusResponse response = ozoneManager.queryUpgradeStatus();

    assertFalse(response.getOmFinalized());
  }

  @Test
  public void testScmStatusPassedThrough() throws IOException {
    HddsProtos.UpgradeStatus scmStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(true)
        .setShouldFinalize(false)
        .setNumDatanodesFinalized(5)
        .setNumDatanodesTotal(5)
        .build();
    when(versionManager.needsFinalization()).thenReturn(false);
    when(containerClient.queryUpgradeStatus()).thenReturn(scmStatus);

    QueryUpgradeStatusResponse response = ozoneManager.queryUpgradeStatus();

    assertEquals(scmStatus, response.getHddsStatus());
  }

  @Test
  public void testScmExceptionPropagates() throws IOException {
    when(versionManager.needsFinalization()).thenReturn(false);
    when(containerClient.queryUpgradeStatus()).thenThrow(new IOException("SCM unreachable"));

    assertThrows(IOException.class, () -> ozoneManager.queryUpgradeStatus());
  }
}
