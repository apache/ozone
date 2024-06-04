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

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents the reply from XceiverClient.
 */
public class XceiverClientReply {

  private CompletableFuture<ContainerCommandResponseProto> response;
  private long logIndex;

  /**
   * Key: Reason -> Value: List of DatanodeDetails
   */
  private final Map<Reason, List<DatanodeDetails>> reasonToNodeListMap;

  /**
   * Indicates the reason datanode is added to the list.
   */
  public enum Reason {
    UNKNOWN,  // The default reason that keeps the original behavior of unmodified callers. TODO: Categorize them
    TIMEOUT,  // Datanode that timed out in ALL_COMMITTED or MAJORITY_COMMITTED request
  }

  public XceiverClientReply(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this.response = response;
    this.reasonToNodeListMap = new ConcurrentHashMap<>();
  }

  public CompletableFuture<ContainerCommandResponseProto> getResponse() {
    return response;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public void setLogIndex(long logIndex) {
    this.logIndex = logIndex;
  }

  public List<DatanodeDetails> getDatanodes() {
    return getDatanodes(Reason.UNKNOWN);
  }

  public List<DatanodeDetails> getDatanodes(Reason reason) {
    return Collections.unmodifiableList(reasonToNodeListMap.get(reason));
  }

  public void addDatanode(DatanodeDetails dn) {
    addDatanode(dn, Reason.UNKNOWN);
  }

  public void addDatanode(DatanodeDetails dn, Reason reason) {
    reasonToNodeListMap.compute(reason, (k, v) -> {
      if (v == null) {
        v = new LinkedList<>();
      }
      v.add(dn);
      return v;
    });
  }

  public void setResponse(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this.response = response;
  }
}
