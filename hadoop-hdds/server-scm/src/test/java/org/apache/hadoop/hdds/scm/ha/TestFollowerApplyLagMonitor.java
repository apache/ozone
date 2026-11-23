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

package org.apache.hadoop.hdds.scm.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FollowerApplyLagMonitor}.
 */
public class TestFollowerApplyLagMonitor {

  @Test
  public void testAllPeersCaughtUp() {
    assertEquals(0, FollowerApplyLagMonitor.applyLag(10, Arrays.asList(10L, 10L, 10L)));
  }

  @Test
  public void testOneSlowFollowerRaisesLag() {
    assertEquals(6, FollowerApplyLagMonitor.applyLag(10, Arrays.asList(10L, 10L, 4L)));
  }

  @Test
  public void testUnreachableFollowerDoesNotRaiseLag() {
    // The unreachable follower is left out of the applied indices.
    assertEquals(0, FollowerApplyLagMonitor.applyLag(10, Arrays.asList(10L, 10L)));
  }

  @Test
  public void testLagUsesSlowestPeer() {
    assertEquals(8, FollowerApplyLagMonitor.applyLag(10, Arrays.asList(10L, 7L, 2L)));
  }

  @Test
  public void testFivePeers() {
    assertEquals(9, FollowerApplyLagMonitor.applyLag(10, Arrays.asList(10L, 9L, 5L, 3L, 1L)));
  }

  @Test
  public void testLagNeverNegative() {
    assertEquals(0, FollowerApplyLagMonitor.applyLag(5, Arrays.asList(8L, 8L, 8L)));
  }

  @Test
  public void testNotLeaderReportsNoLag() {
    final RaftServer.Division division = mock(RaftServer.Division.class);
    final DivisionInfo info = mock(DivisionInfo.class);
    when(division.getInfo()).thenReturn(info);
    when(info.isLeader()).thenReturn(false);

    try (FollowerApplyLagMonitor monitor = new FollowerApplyLagMonitor(division, null, 1000)) {
      monitor.refresh();
      assertEquals(0, monitor.getLag());
    }
  }

  @Test
  public void testFailedRefreshKeepsPreviousLag() {
    final RaftServer.Division division = mock(RaftServer.Division.class);
    final DivisionInfo info = mock(DivisionInfo.class);
    when(division.getInfo()).thenReturn(info);
    when(info.isLeader()).thenReturn(true);
    final AtomicBoolean fail = new AtomicBoolean();

    try (FollowerApplyLagMonitor monitor = new FollowerApplyLagMonitor(division, null, 1000) {
      @Override
      long measure() throws IOException {
        if (fail.get()) {
          throw new IOException("Only 1 of 3 SCM peers reported their applied index");
        }
        return 7;
      }
    }) {
      monitor.refresh();
      assertEquals(7, monitor.getLag());

      // A poll that throws must not change the cached value.
      fail.set(true);
      monitor.refresh();
      assertEquals(7, monitor.getLag());
    }
  }
}
