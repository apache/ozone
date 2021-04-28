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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Exception thrown when a server is not a leader for Ratis group.
 */
public class ServerNotLeaderException extends IOException {
  private final String currentPeerId;
  private final String leader;
  private static final Pattern CURRENT_PEER_ID_PATTERN =
      Pattern.compile("Server:(.*) is not the leader[.]+.*", Pattern.DOTALL);
  private static final Pattern SUGGESTED_LEADER_PATTERN =
      Pattern.compile(".*Suggested leader is Server:([^:]*)(:[0-9]+).*",
          Pattern.DOTALL);

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

  public ServerNotLeaderException(String message) {
    super(message);

    Matcher currentLeaderMatcher = CURRENT_PEER_ID_PATTERN.matcher(message);
    if (currentLeaderMatcher.matches()) {
      this.currentPeerId = currentLeaderMatcher.group(1);

      Matcher suggestedLeaderMatcher =
          SUGGESTED_LEADER_PATTERN.matcher(message);
      if (suggestedLeaderMatcher.matches()) {
        if (suggestedLeaderMatcher.groupCount() == 2) {
          if (suggestedLeaderMatcher.group(1).isEmpty()
              || suggestedLeaderMatcher.group(2).isEmpty()) {
            this.leader = null;
          } else {
            this.leader = suggestedLeaderMatcher.group(1) +
                suggestedLeaderMatcher.group(2);
          }
        } else {
          this.leader = null;
        }
      } else {
        this.leader = null;
      }
    } else {
      this.currentPeerId = null;
      this.leader = null;
    }
  }

  public String getSuggestedLeader() {
    return leader;
  }

  /**
   * Convert {@link org.apache.ratis.protocol.exceptions.NotLeaderException} 
   * to {@link ServerNotLeaderException}.
   * @param notLeaderException
   * @param currentPeer
   * @return ServerNotLeaderException
   */
  public static ServerNotLeaderException convertToNotLeaderException(
      NotLeaderException notLeaderException,
      RaftPeerId currentPeer, String port) {
    String suggestedLeader = notLeaderException.getSuggestedLeader() != null ?
        HddsUtils
            .getHostName(notLeaderException.getSuggestedLeader().getAddress())
            .get() :
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