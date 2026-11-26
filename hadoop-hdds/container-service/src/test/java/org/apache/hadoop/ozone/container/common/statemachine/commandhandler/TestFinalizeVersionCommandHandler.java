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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.protocol.commands.FinalizeVersionCommand;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FinalizeVersionCommandHandler}.
 */
public class TestFinalizeVersionCommandHandler {

  private static final int DN_SOFTWARE_VERSION = 3;

  @AfterEach
  public void reset() {
    ExitUtils.clear();
  }

  private StateContext mockContext(DatanodeVersionManager versionManager) {
    ComponentVersion softwareVersion = mock(ComponentVersion.class);
    when(softwareVersion.serialize()).thenReturn(DN_SOFTWARE_VERSION);
    when(versionManager.getSoftwareVersion()).thenReturn(softwareVersion);

    DatanodeStateMachine dsm = mock(DatanodeStateMachine.class);
    when(dsm.getVersionManager()).thenReturn(versionManager);
    StateContext context = mock(StateContext.class);
    when(context.getParent()).thenReturn(dsm);
    return context;
  }

  @Test
  public void testFinalizesWhenExpectedVersionMatches() throws UpgradeException {
    DatanodeVersionManager versionManager = mock(DatanodeVersionManager.class);
    when(versionManager.needsFinalization()).thenReturn(true);
    StateContext context = mockContext(versionManager);

    new FinalizeVersionCommandHandler().handle(
        new FinalizeVersionCommand(DN_SOFTWARE_VERSION), mock(OzoneContainer.class),
        context, mock(SCMConnectionManager.class));

    verify(versionManager, times(1)).finalizeUpgrade();
  }

  @Test
  public void testTerminatesWhenExpectedVersionMismatches() throws UpgradeException {
    ExitUtils.disableSystemExit();
    DatanodeVersionManager versionManager = mock(DatanodeVersionManager.class);
    StateContext context = mockContext(versionManager);

    ExitUtils.ExitException ex = assertThrows(ExitUtils.ExitException.class, () ->
        new FinalizeVersionCommandHandler().handle(
            new FinalizeVersionCommand(DN_SOFTWARE_VERSION + 1), mock(OzoneContainer.class),
            context, mock(SCMConnectionManager.class)));

    assertEquals(1, ex.getStatus());
    verify(versionManager, never()).finalizeUpgrade();
  }
}
