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

package org.apache.hadoop.ozone.om.balancing;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Info about multi raft group leader changing.
 */
public class LeaderChangingGroupInfo {
  private final RaftGroupId groupId;
  private final RaftPeerId oldLeader;
  private final RaftPeerId newLeader;

  public LeaderChangingGroupInfo(RaftGroupId groupId, RaftPeerId oldLeader, RaftPeerId newLeader) {
    this.groupId = groupId;
    this.oldLeader = oldLeader;
    this.newLeader = newLeader;
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  public RaftPeerId getOldLeader() {
    return oldLeader;
  }

  public RaftPeerId getNewLeader() {
    return newLeader;
  }

  @Override
  public String toString() {
    return String.format(
        "LeaderChangingGroupInfo{groupId= %s, oldLeader = %s, newLeader = %s}",
        groupId, oldLeader, newLeader
    );
  }
}
