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

package org.apache.hadoop.hdds.scm;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;

/**
 * This class represents the reply from XceiverClient.
 */
public class XceiverClientReply {

  private CompletableFuture<ContainerCommandResponseProto> response;
  private long logIndex;

  /**
   * List of datanodes where the command got executed and reply is received.
   * If there is an exception in the reply, these datanodes will inform
   * about the servers where there is a failure.
   */
  private final List<DatanodeDetails> datanodes;

  public XceiverClientReply(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this.response = response;
    this.datanodes = new LinkedList<>();
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
    return Collections.unmodifiableList(datanodes);
  }

  public void addDatanode(DatanodeDetails dn) {
    datanodes.add(dn);
  }

  public void setResponse(
      CompletableFuture<ContainerCommandResponseProto> response) {
    this.response = response;
  }
}
