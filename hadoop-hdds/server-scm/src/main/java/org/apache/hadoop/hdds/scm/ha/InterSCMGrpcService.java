/*
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointRequestProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointResponseProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolServiceGrpc;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to handle Rocks db Checkpointing.
 */
public class InterSCMGrpcService extends
    InterSCMProtocolServiceGrpc.InterSCMProtocolServiceImplBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(InterSCMGrpcService.class);

  private static final int BUFFER_SIZE = 1024 * 1024;

  private final String clusterId;
  private final SCMDBCheckpointProvider provider;

  public InterSCMGrpcService(final StorageContainerManager scm) {
    Preconditions.checkNotNull(scm);
    this.clusterId = scm.getClusterId();
    provider =
        new SCMDBCheckpointProvider(scm.getScmMetadataStore().getStore());
  }

  @Override
  public void download(CopyDBCheckpointRequestProto request,
      StreamObserver<CopyDBCheckpointResponseProto> responseObserver) {
    try {
      SCMGrpcOutputStream outputStream =
          new SCMGrpcOutputStream(responseObserver, clusterId, BUFFER_SIZE);
      provider.writeDBCheckPointToSream(outputStream, request.getFlush());

    } catch (IOException e) {
      LOG.error("Error streaming SCM DB checkpoint", e);
      responseObserver.onError(e);
    }
  }
}
