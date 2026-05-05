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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the ReplicationManagerEventHandler class.
 */
public class TestReplicationManagerEventHandler {
  private ReplicationManager replicationManager;
  private ReplicationManagerEventHandler replicationManagerEventHandler;
  private EventPublisher publisher;
  private SCMContext scmContext;

  @BeforeEach
  public void setUp() {
    replicationManager = mock(ReplicationManager.class);
    publisher = mock(EventPublisher.class);
    scmContext = mock(SCMContext.class);
    replicationManagerEventHandler = new ReplicationManagerEventHandler(replicationManager, scmContext);
  }

  private static Stream<Arguments> testData() {
    return Stream.of(
      Arguments.of(true, false, true),
      Arguments.of(false, true, false),
      Arguments.of(true, true, false),
      Arguments.of(false, false, false)
    );
  }

  @ParameterizedTest
  @MethodSource("testData")
  public void testReplicationManagerEventHandler(boolean isLeaderReady, boolean isInSafeMode,
      boolean isExpectedToNotify) {
    when(scmContext.isLeaderReady()).thenReturn(isLeaderReady);
    when(scmContext.isInSafeMode()).thenReturn(isInSafeMode);
    DatanodeDetails dataNodeDetails = MockDatanodeDetails.randomDatanodeDetails();
    replicationManagerEventHandler.onMessage(dataNodeDetails, publisher);

    verify(replicationManager, times(isExpectedToNotify ? 1 : 0)).notifyNodeStateChange();
  }
}
