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

package org.apache.hadoop.ozone.om.jobworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.OptionalLong;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerCommandManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link OMJobworkerLeaderTracker} drops dispatch state when leadership or the term changes.
 */
class TestOMJobworkerLeaderTracker {
  private DivisionInfo info;
  private OMJobworkerCommandManager commandManager;
  private JobworkerNodeManager nodeManager;
  private OMJobworkerLeaderTracker tracker;

  @BeforeEach
  void setUp() {
    info = mock(DivisionInfo.class);
    RaftServer.Division division = mock(RaftServer.Division.class);
    when(division.getInfo()).thenReturn(info);
    OzoneManagerRatisServer ratisServer = mock(OzoneManagerRatisServer.class);
    when(ratisServer.getServerDivision()).thenReturn(division);
    OzoneManager om = mock(OzoneManager.class);
    when(om.getOmRatisServer()).thenReturn(ratisServer);
    commandManager = mock(OMJobworkerCommandManager.class);
    nodeManager = mock(JobworkerNodeManager.class);
    tracker = new OMJobworkerLeaderTracker(om);
  }

  private void setLeader(boolean leader, boolean ready, long term) {
    when(info.isLeader()).thenReturn(leader);
    when(info.isLeaderReady()).thenReturn(leader && ready);
    when(info.getCurrentTerm()).thenReturn(term);
  }

  @Test
  void testTermIsOnlyAvailableOnTheReadyLeader() {
    setLeader(false, false, 3);
    assertFalse(tracker.getTermIfReady().isPresent());
    assertFalse(tracker.getTermIfLeader().isPresent());

    setLeader(true, false, 3);
    assertFalse(tracker.isLeaderReady());
    assertEquals(OptionalLong.of(3), tracker.getTermIfLeader());

    setLeader(true, true, 3);
    assertTrue(tracker.isLeaderReady());
    assertEquals(OptionalLong.of(3), tracker.getTermIfReady());
  }

  @Test
  void testBecomingLeaderDoesNotDropState() {
    setLeader(false, false, 3);
    tracker.attach(commandManager, nodeManager);
    setLeader(true, true, 4);
    tracker.notifyStatusChanged();
    verify(commandManager, never()).onLeadershipChange();
    verify(nodeManager, never()).clearAllCommands();
  }

  @Test
  void testLosingLeadershipOrNewTermDropsState() {
    setLeader(true, true, 4);
    tracker.attach(commandManager, nodeManager);
    tracker.notifyStatusChanged();
    verify(commandManager, never()).onLeadershipChange();

    setLeader(false, false, 5);
    tracker.notifyStatusChanged();
    verify(commandManager, times(1)).onLeadershipChange();
    verify(nodeManager, times(1)).clearAllCommands();

    setLeader(true, true, 6);
    tracker.notifyStatusChanged();
    setLeader(true, true, 7);
    tracker.notifyStatusChanged();
    verify(commandManager, times(2)).onLeadershipChange();
    verify(nodeManager, times(2)).clearAllCommands();
  }

  @Test
  void testDetachedTrackerDoesNothing() {
    setLeader(true, true, 4);
    tracker.attach(commandManager, nodeManager);
    tracker.detach();
    setLeader(false, false, 5);
    tracker.notifyStatusChanged();
    verify(commandManager, never()).onLeadershipChange();
  }
}
