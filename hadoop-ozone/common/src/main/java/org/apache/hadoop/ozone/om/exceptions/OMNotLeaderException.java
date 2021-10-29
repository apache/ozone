/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.exceptions;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Exception thrown by
 * {@link org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB} when
 * a read request is received by a non leader OM node.
 */
public class OMNotLeaderException extends IOException {

  private final String leaderPeerAddress;
  private static final Pattern CURRENT_PEER_ID_PATTERN =
      Pattern.compile("OM:(.*) is not the leader[.]+.*", Pattern.DOTALL);
  private static final Pattern SUGGESTED_LEADER_PATTERN =
      Pattern.compile(".*Suggested leader is OM:([^.]*).*", Pattern.DOTALL);

  public OMNotLeaderException(RaftPeerId currentPeerId) {
    super("OM:" + currentPeerId + " is not the leader. Could not " +
        "determine the leader node.");
    this.leaderPeerAddress = null;
  }

  public OMNotLeaderException(RaftPeerId currentPeerId,
                              String suggestedLeaderAddress) {
    super("OM:" + currentPeerId + " is not the leader. Suggested leader " +
        " Address is:" + suggestedLeaderAddress + ".");
    this.leaderPeerAddress = suggestedLeaderAddress;
  }

  public String getSuggestedLeaderAddress() {
    return leaderPeerAddress;
  }

  /**
   * Convert {@link NotLeaderException} to {@link OMNotLeaderException}.
   * @param notLeaderException
   * @param currentPeer
   * @return OMNotLeaderException
   */
  public static OMNotLeaderException convertToOMNotLeaderException(
      NotLeaderException notLeaderException, RaftPeerId currentPeer) {
    RaftPeer suggestedLeader =
        notLeaderException.getSuggestedLeader() != null ?
            notLeaderException.getSuggestedLeader() : null;
    OMNotLeaderException omNotLeaderException;
    if (suggestedLeader != null) {
      omNotLeaderException = new OMNotLeaderException(currentPeer,
          suggestedLeader.getAddress());
    } else {
      omNotLeaderException =
          new OMNotLeaderException(currentPeer);
    }
    return omNotLeaderException;
  }
}
