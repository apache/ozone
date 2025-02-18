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
package org.apache.hadoop.ozone.om.bucket.server;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.util.List;

/** A server endpoint that acts as the communication layer for Ozone
 * buckets. */
public interface XbeiverServerSpi {

  /** Starts the server. */
  void start() throws IOException;

  /** Stops a running server. */
  void stop();

  /**
   * submits a containerRequest to be performed by the replication pipeline.
   * @param request ContainerCommandRequest
   */
  OzoneManagerProtocolProtos.OMResponse submitRequest(OzoneManagerProtocolProtos.OMRequest omRequest)
          throws ServiceException;

  /**
   * Returns true if the given pipeline exist.
   *
   * @return true if pipeline present, else false
   */
  boolean isExist(RaftGroupId pipelineId);

  /**
   * Join a new pipeline with priority.
   */
  default void addGroup(RaftGroupId groupId,
                        List<RaftPeer> peers,
                        List<Integer> priorityList) throws IOException {
  }

  /**
   * Exit a pipeline.
   */
  default void removeGroup(HddsProtos.PipelineID pipelineId)
      throws IOException {
  }
}
