/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for the ScmBlockLocationProtocolServerSideTranslatorPB.
 */
public class TestScmBlockLocationProtocolServerSideTranslatorPB {

  @Test
  public void testCheckIfAllDNInExcludeList() throws Exception {
    // create a list of DN managed by SCM
    List<DatanodeDetails> datanodeDetailsList = new ArrayList<>();
    datanodeDetailsList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());
    datanodeDetailsList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());
    datanodeDetailsList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
            .build());

    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.getAllNodes()).thenReturn(datanodeDetailsList);

    StorageContainerManager storageContainerManager = mock(
        StorageContainerManager.class);
    when(storageContainerManager.getScmNodeManager()).thenReturn(nodeManager);

    ScmBlockLocationProtocolServerSideTranslatorPB
        scmBlockLocationProtocolServerSideTranslatorPB
        = new ScmBlockLocationProtocolServerSideTranslatorPB(
            mock(ScmBlockLocationProtocol.class),
            storageContainerManager,
            mock(ProtocolMessageMetrics.class));

    ExcludeList excludeList = mock(ExcludeList.class);
    Set<DatanodeDetails> excludeListDN = new HashSet<>();
    excludeListDN.addAll(datanodeDetailsList);
    when(excludeList.getDatanodes()).thenReturn(excludeListDN);

    SCMException scmException = assertThrows(SCMException.class,
        () -> scmBlockLocationProtocolServerSideTranslatorPB
                .checkIfAllDNInExcludeList(excludeList.getDatanodes()));
    assertTrue(scmException.getMessage()
        .contains("All Datanodes have been added in exclude list." +
            " Client should retry all Datanodes again."));
    assertEquals(scmException.getResult(),
        SCMException.ResultCodes.RETRY_ALL_DN_IN_EXCLUDE_LIST);
  }

}
