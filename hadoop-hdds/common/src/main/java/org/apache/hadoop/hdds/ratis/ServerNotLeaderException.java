/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.ratis;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

import java.io.IOException;

/**
 * Exception thrown when a server is not a leader for Ratis group.
 */
public class ServerNotLeaderException extends IOException {
  private final String currentPeerId;
  private final String leader;

  public ServerNotLeaderException(RaftPeerId currentPeerId) {
    super("Server:" + currentPeerId + " is not the leader. Could not " +
        "determine the leader node.");
    this.currentPeerId = currentPeerId.toString();
    this.leader = null;
  }

  public ServerNotLeaderException(RaftPeerId currentPeerId,
      String suggestedLeader) {
    super("Server:" + currentPeerId + " is not the leader. Suggested leader is"
        + " Server:" + suggestedLeader + ".");
    this.currentPeerId = currentPeerId.toString();
    this.leader = suggestedLeader;
  }

  public String getSuggestedLeader() {
    return leader;
  }

  /**
   * Convert {@link org.apache.ratis.protocol.exceptions.NotLeaderException} 
   * to {@link ServerNotLeaderException}.
   */
  public static ServerNotLeaderException convertToNotLeaderException(
      NotLeaderException notLeaderException,
      RaftPeerId currentPeer, String port) {
    String suggestedLeader = notLeaderException.getSuggestedLeader() != null ?
        HddsUtils
            .getHostName(notLeaderException.getSuggestedLeader().getAddress())
            .orElse(null) :
        null;
    ServerNotLeaderException serverNotLeaderException;
    if (suggestedLeader != null) {
      String suggestedLeaderHostPort = suggestedLeader + ":" + port;
      serverNotLeaderException =
          new ServerNotLeaderException(currentPeer, suggestedLeaderHostPort);
    } else {
      serverNotLeaderException = new ServerNotLeaderException(currentPeer);
    }
    return serverNotLeaderException;
  }
}
