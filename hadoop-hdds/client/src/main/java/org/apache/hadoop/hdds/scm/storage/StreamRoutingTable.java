/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to get the RoutingTable for streaming.
 */
public final class StreamRoutingTable {

  public static final Logger LOG =
      LoggerFactory.getLogger(StreamRoutingTable.class);

  private StreamRoutingTable() {
  }

  public static RoutingTable getRoutingTable(Pipeline pipeline) {
    RaftPeerId primaryId = null;
    List<RaftPeerId> raftPeers = new ArrayList<>();

    for (DatanodeDetails dn : pipeline.getNodes()) {
      final RaftPeerId raftPeerId = RaftPeerId.valueOf(dn.getUuidString());
      try {
        if (dn == pipeline.getFirstNode()) {
          primaryId = raftPeerId;
        }
      } catch (IOException e) {
        LOG.error("Can not get FirstNode from the pipeline: {} with " +
            "exception: {}", pipeline.toString(), e.getLocalizedMessage());
        return null;
      }
      raftPeers.add(raftPeerId);
    }

    RoutingTable.Builder builder = RoutingTable.newBuilder();
    RaftPeerId previousId = primaryId;
    for (RaftPeerId peerId : raftPeers) {
      if (peerId.equals(primaryId)) {
        continue;
      }
      builder.addSuccessor(previousId, peerId);
      previousId = peerId;
    }

    return builder.build();
  }
}
