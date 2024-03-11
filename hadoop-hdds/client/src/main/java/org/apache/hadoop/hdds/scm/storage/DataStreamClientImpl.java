/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.XceiverClientSpi.Validator;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.client.api.DataStreamOutput;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming client implementation
 * allows client to create streams and send asynchronously.
 */
public class DataStreamClientImpl {

  public DataStreamInput streamReadOnly(
      ContainerCommandRequestProto request, XceiverClientSpi client,
      List<Validator> validators) {
    return new DataStreamInput(request, client, validators);
  }

  /** An asynchronous input stream. */
  public final class DataStreamInput {
    private ContainerCommandRequestProto request;
    private XceiverClientSpi client;
    private List<Validator> validators;

    DataStreamInput(
        ContainerCommandRequestProto request, XceiverClientSpi client,
        List<Validator> validators) {
      this.request = request;
      this.client = client;
      this.validators = validators;
    }
    CompletableFuture<DatanodeBlockID> read(ByteBuffer buffer) {
      try {
        return ((XceiverClientGrpc) client).sendCommandOnlyRead(request, buffer, validators);
      } catch (SCMSecurityException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
