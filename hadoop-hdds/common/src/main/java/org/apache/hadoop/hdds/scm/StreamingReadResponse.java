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

  public StreamingReadResponse(DatanodeDetails dn,
      ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver) {
    this.dn = dn;
    this.requestObserver = requestObserver;

    final String s = dn.getID().toString();
    this.name = "dn" + s.substring(s.lastIndexOf('-')) + "_stream";
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
