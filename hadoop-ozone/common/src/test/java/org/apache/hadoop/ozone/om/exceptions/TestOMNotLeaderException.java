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

package org.apache.hadoop.ozone.om.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.logging.log4j.internal.annotation.SuppressFBWarnings;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

/**
 * Tests for OMNotLeaderException.
 */
@SuppressFBWarnings(value = "NM_CLASS_NOT_EXCEPTION")
public class TestOMNotLeaderException {

  @Test
  public void testStringParametrizedConstructor() {
    // given
    RaftGroupId raftGroupId = RaftGroupId.valueOf(UUID.randomUUID());
    OMNotLeaderException exception = new OMNotLeaderException(RaftPeerId.valueOf("om1"), RaftPeerId.valueOf("om2"),
        "om2:9876", raftGroupId);

    // when
    OMNotLeaderException parsedException = new OMNotLeaderException(exception.getMessage());

    // then
    assertEquals(exception.getCurrentPeerId(), parsedException.getCurrentPeerId());
    assertEquals(exception.getSuggestedLeaderNodeId(), parsedException.getSuggestedLeaderNodeId());
    assertEquals(exception.getSuggestedLeaderAddress(), parsedException.getSuggestedLeaderAddress());
    assertEquals(exception.getRaftGroupId(), parsedException.getRaftGroupId());
  }

}
