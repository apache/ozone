/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Test for StorageContainerDatanodeProtocolProtos.
 */
public class TestSCMDatanodeProtocolServer {

  @Test
  public void ensureTermAndDeadlineOnCommands()
      throws IOException, TimeoutException {
    OzoneStorageContainerManager scm =
        Mockito.mock(OzoneStorageContainerManager.class);

    ReplicateContainerCommand command = ReplicateContainerCommand.forTest(1);
    command.setTerm(5L);
    command.setDeadline(1234L);
    StorageContainerDatanodeProtocolProtos.SCMCommandProto proto =
        SCMDatanodeProtocolServer.getCommandResponse(command, scm);

    Assert.assertEquals(StorageContainerDatanodeProtocolProtos.SCMCommandProto
        .Type.replicateContainerCommand, proto.getCommandType());
    Assert.assertEquals(5L, proto.getTerm());
    Assert.assertEquals(1234L, proto.getDeadlineMsSinceEpoch());
  }
}
