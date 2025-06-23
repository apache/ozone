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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * Exception thrown by
 * {@link org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB} when
 * a read request is received by a non leader OM node.
 */
public class OMNotLeaderException extends IOException {

  private final String leaderPeerId;
  private final String leaderAddress;

  public OMNotLeaderException(RaftPeerId currentPeerId) {
    super("OM:" + currentPeerId + " is not the leader. Could not " +
        "determine the leader node.");
    this.leaderPeerId = null;
    this.leaderAddress = null;
  }

  public OMNotLeaderException(RaftPeerId currentPeerId,
      RaftPeerId suggestedLeaderPeerId) {
    this(currentPeerId, suggestedLeaderPeerId, null);
  }

  public OMNotLeaderException(RaftPeerId currentPeerId,
      RaftPeerId suggestedLeaderPeerId, String suggestedLeaderAddress) {
    super("OM:" + currentPeerId + " is not the leader. Suggested leader is" +
        " OM:" + suggestedLeaderPeerId + "[" + suggestedLeaderAddress + "].");
    this.leaderPeerId = suggestedLeaderPeerId.toString();
    this.leaderAddress = suggestedLeaderAddress;
  }

  public OMNotLeaderException(String msg) {
    super(msg);
    this.leaderPeerId = null;
    this.leaderAddress = null;
  }

  public String getSuggestedLeaderNodeId() {
    return leaderPeerId;
  }

  public String getSuggestedLeaderAddress() {
    return leaderAddress;
  }

  /**
   * Convert {@link NotLeaderException} to {@link OMNotLeaderException}.
   * @param notLeaderException
   * @param currentPeer
   * @return OMNotLeaderException
   */
  public static OMNotLeaderException convertToOMNotLeaderException(
      NotLeaderException notLeaderException, RaftPeerId currentPeer) {
    RaftPeerId suggestedLeader =
        notLeaderException.getSuggestedLeader() != null ?
            notLeaderException.getSuggestedLeader().getId() : null;
    String suggestedLeaderAddress =
        notLeaderException.getSuggestedLeader() != null ?
            notLeaderException.getSuggestedLeader().getAddress() : null;
    OMNotLeaderException omNotLeaderException;
    if (suggestedLeader != null) {
      omNotLeaderException = new OMNotLeaderException(currentPeer,
          suggestedLeader, suggestedLeaderAddress);
    } else {
      omNotLeaderException =
          new OMNotLeaderException(currentPeer);
    }
    return omNotLeaderException;
  }
}
