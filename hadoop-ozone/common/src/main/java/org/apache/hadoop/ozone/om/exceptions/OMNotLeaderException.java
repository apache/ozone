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

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * Exception thrown by
 * {@link org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB} when
 * a read request is received by a non leader OM node.
 */
public class OMNotLeaderException extends IOException {

  private final String currentPeerId;
  private final String leaderPeerId;
  private final String leaderAddress;
  private final RaftGroupId raftGroupId;

  private static final Pattern EXCEPTION_PATTERN =
      Pattern.compile("^OM:(\\w+)\\s+is not the leader for raft group\\s+\\[([0-9a-fA-F-]+)\\]\\.\\s+Suggested leader "
          + "is OM:(.+)\\[(.+)\\].*$");

  public OMNotLeaderException(String message) {
    super(message);
    String firstLine = message.split("\n")[0];
    Matcher matcher = EXCEPTION_PATTERN.matcher(firstLine);
    if (matcher.matches()) {
      this.currentPeerId = matcher.group(1);
      this.leaderPeerId = matcher.group(3);
      this.leaderAddress = matcher.group(4);
      this.raftGroupId = RaftGroupId.valueOf(UUID.fromString(matcher.group(2)));
    } else {
      this.currentPeerId = null;
      this.leaderPeerId = null;
      this.leaderAddress = null;
      this.raftGroupId = null;
    }
  }

  public OMNotLeaderException(RaftPeerId currentPeerId, RaftGroupId raftGroupId) {
    super("OM:" + currentPeerId + " is not the leader. Could not " +
        "determine the leader node.");
    this.currentPeerId = currentPeerId.toString();
    this.leaderPeerId = null;
    this.leaderAddress = null;
    this.raftGroupId = raftGroupId;
  }

  public OMNotLeaderException(RaftPeerId currentPeerId) {
    this(currentPeerId, null);
  }

  public OMNotLeaderException(RaftPeerId currentPeerId,
      RaftPeerId suggestedLeaderPeerId, RaftGroupId raftGroupId) {
    this(currentPeerId, suggestedLeaderPeerId, null, raftGroupId);
  }

  public OMNotLeaderException(RaftPeerId currentPeerId,
      RaftPeerId suggestedLeaderPeerId, String suggestedLeaderAddress, RaftGroupId raftGroupId) {
    super("OM:" + currentPeerId + " is not the leader for raft group [" + raftGroupId.getUuid().toString()
        + "]. Suggested leader is" +
        " OM:" + suggestedLeaderPeerId + "[" + suggestedLeaderAddress + "].");
    this.leaderPeerId = suggestedLeaderPeerId.toString();
    this.leaderAddress = suggestedLeaderAddress;
    this.raftGroupId = raftGroupId;
    this.currentPeerId = currentPeerId.toString();
  }

  public String getSuggestedLeaderNodeId() {
    return leaderPeerId;
  }

  public String getSuggestedLeaderAddress() {
    return leaderAddress;
  }

  public RaftGroupId getRaftGroupId() {
    return raftGroupId;
  }

  public String getCurrentPeerId() {
    return currentPeerId;
  }

  /**
   * Convert {@link NotLeaderException} to {@link OMNotLeaderException}.
   *
   * @param notLeaderException
   * @param currentPeer
   * @param raftGroupId
   * @return OMNotLeaderException
   */
  public static OMNotLeaderException convertToOMNotLeaderException(
      NotLeaderException notLeaderException, RaftPeerId currentPeer, RaftGroupId raftGroupId) {
    RaftPeerId suggestedLeader =
        notLeaderException.getSuggestedLeader() != null ?
            notLeaderException.getSuggestedLeader().getId() : null;
    String suggestedLeaderAddress =
        notLeaderException.getSuggestedLeader() != null ?
            notLeaderException.getSuggestedLeader().getAddress() : null;
    OMNotLeaderException omNotLeaderException;
    if (suggestedLeader != null) {
      omNotLeaderException = new OMNotLeaderException(currentPeer,
          suggestedLeader, suggestedLeaderAddress, raftGroupId);
    } else {
      omNotLeaderException =
          new OMNotLeaderException(currentPeer, raftGroupId);
    }
    return omNotLeaderException;
  }
}
