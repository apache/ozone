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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;

/**
 * Streaming read response holding datanode details and
 * request observer to send read requests.
 */
public class StreamingReadResponse {

  private final DatanodeDetails dn;
  private final ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver;
  private final String name;

  /**
   * Deadline (a {@link System#nanoTime} value) bounding the current wait, used as a single read budget
   * for the call: set before streamRead() to bound the isReady() flow-control wait, then refreshed
   * afterwards to bound the response wait in poll(). {@link #readDeadlineSet} tracks whether it has been
   * set, since nanoTime values may be zero or negative and cannot be used as a sentinel.
   */
  private volatile long readDeadlineNs;
  private volatile boolean readDeadlineSet;

  public StreamingReadResponse(DatanodeDetails dn,
      ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver) {
    this.dn = dn;
    this.requestObserver = requestObserver;

    final String s = dn.getID().toString();
    this.name = "dn" + s.substring(s.lastIndexOf('-')) + "_stream";
  }

  public void setReadDeadlineNs(long deadlineNs) {
    this.readDeadlineNs = deadlineNs;
    this.readDeadlineSet = true;
  }

  public long getReadDeadlineNs() {
    return readDeadlineNs;
  }

  public boolean hasReadDeadline() {
    return readDeadlineSet;
  }

  public DatanodeDetails getDatanodeDetails() {
    return dn;
  }

  public ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> getRequestObserver() {
    return requestObserver;
  }

  @Override
  public String toString() {
    return name;
  }
}
