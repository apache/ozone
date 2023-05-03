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
import java.util.Arrays;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointRequestProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointResponseProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolServiceGrpc;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

/**
 * Service to handle Rocks db Checkpointing.
 */
public class InterSCMGrpcService extends
    InterSCMProtocolServiceGrpc.InterSCMProtocolServiceImplBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(InterSCMGrpcService.class);

  private static final int BUFFER_SIZE = 1024 * 1024;

  private final SCMDBCheckpointProvider provider;

  private final StorageContainerManager scm;

  public InterSCMGrpcService(final StorageContainerManager scm) {
    Preconditions.checkNotNull(scm);
    this.scm = scm;
    provider =
        new SCMDBCheckpointProvider(scm.getScmMetadataStore().getStore());
  }

  @Override
  public void download(CopyDBCheckpointRequestProto request,
      StreamObserver<CopyDBCheckpointResponseProto> responseObserver) {
    try {
      scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
      Table<String, TransactionInfo> transactionInfoTable =
          Arrays.stream(new SCMDBDefinition().getColumnFamilies())
              .filter(t -> t.getValueType() == TransactionInfo.class)
              .findFirst().get().getTable(scm.getScmMetadataStore().getStore());

      TransactionInfo transactionInfo =
          transactionInfoTable.get(TRANSACTION_INFO_KEY);
      Preconditions.checkNotNull(transactionInfo);
      SCMGrpcOutputStream outputStream =
          new SCMGrpcOutputStream(responseObserver, scm.getClusterId(),
              BUFFER_SIZE);
      provider.writeDBCheckPointToSream(outputStream, request.getFlush());

    } catch (IOException e) {
      LOG.error("Error streaming SCM DB checkpoint", e);
      responseObserver.onError(e);
    }
  }
}
