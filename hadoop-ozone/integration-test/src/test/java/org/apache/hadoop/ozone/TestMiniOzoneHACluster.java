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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.BindException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl.SCMHAService;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

/**
 * Tests the SCM bootstrap readiness guarantees of the HA mini cluster.
 */
@Timeout(30)
class TestMiniOzoneHACluster {

  @TempDir
  private Path testDir;

  @ParameterizedTest
  @CsvSource({"true, false", "false, false", "true, true", "false, true"})
  void testBootstrapSCMWaitsForReadiness(boolean updateConfigs, boolean portConflict) throws Exception {
    StorageContainerManager leader = mock(StorageContainerManager.class, RETURNS_DEEP_STUBS);
    StorageContainerManager newSCM = mock(StorageContainerManager.class, RETURNS_DEEP_STUBS);
    when(leader.getSCMNodeId()).thenReturn("scm1");
    when(leader.getScmId()).thenReturn("leader-scm-id");
    when(leader.checkLeader()).thenReturn(true);
    when(leader.getScmHAManager().getRatisServer().getSCMStateMachine().getLatestSnapshot().getIndex())
        .thenReturn(10L);
    when(newSCM.getSCMNodeId()).thenReturn("scm2");
    when(newSCM.getScmId()).thenReturn("new-scm-id");
    SnapshotInfo snapshot = newSCM.getScmHAManager().getRatisServer().getSCMStateMachine().getLatestSnapshot();
    when(snapshot.getIndex()).thenReturn(9L, 10L);
    when(leader.doesPeerExist(anyString())).thenReturn(true);
    when(newSCM.doesPeerExist(anyString())).thenReturn(true);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_NAMES, "localhost:1234");
    conf.set(ScmConfigKeys.OZONE_SCM_NODES_KEY + ".scm-service", "scm1");
    SCMHAService scmService = new SCMHAService(new ArrayList<>(Collections.singletonList(leader)),
        new ArrayList<>(), "scm-service");
    MiniOzoneHAClusterImpl cluster = new MiniOzoneHAClusterImpl(conf, null, null, scmService,
        Collections.emptyList(), testDir.toString(), Collections.emptyList());

    try (MockedStatic<StorageContainerManager> scmFactory = mockStatic(StorageContainerManager.class)) {
      if (portConflict) {
        scmFactory.when(() -> StorageContainerManager.createSCM(any(OzoneConfiguration.class)))
            .thenThrow(new BindException("Port already in use")).thenReturn(newSCM);
      } else {
        scmFactory.when(() -> StorageContainerManager.createSCM(any(OzoneConfiguration.class))).thenReturn(newSCM);
      }

      cluster.bootstrapSCM("scm2", updateConfigs);

      verify(snapshot, times(2)).getIndex();
      if (updateConfigs) {
        verify(leader).doesPeerExist(newSCM.getScmId());
        verify(newSCM).doesPeerExist(leader.getScmId());
      } else {
        verify(leader, never()).doesPeerExist(anyString());
        verify(newSCM, never()).doesPeerExist(anyString());
      }
      scmFactory.verify(() -> StorageContainerManager.createSCM(any(OzoneConfiguration.class)),
          times(portConflict ? 2 : 1));
    }
  }
}
