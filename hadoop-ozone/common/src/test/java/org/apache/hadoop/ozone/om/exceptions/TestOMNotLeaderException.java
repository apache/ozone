package org.apache.hadoop.ozone.om.exceptions;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for OMNotLeaderException.
 */
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
