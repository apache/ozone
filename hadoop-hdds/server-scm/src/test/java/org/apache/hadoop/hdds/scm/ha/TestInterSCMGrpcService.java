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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.File;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointRequestProto;
import org.apache.hadoop.hdds.protocol.scm.proto.InterSCMProtocolProtos.CopyDBCheckpointResponseProto;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMPerformanceMetrics;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link InterSCMGrpcService}.
 */
class TestInterSCMGrpcService {

  private static final TransactionInfo TRX_INFO_T4 =
      TransactionInfo.valueOf(1, 4);
  private static final TransactionInfo TRX_INFO_T5 =
      TransactionInfo.valueOf(1, 5);

  @TempDir
  private File testDir;

  private final AtomicLong clockMillis = new AtomicLong();
  private SCMMetadataStore metadataStore;
  private SCMHADBTransactionBufferImpl transactionBuffer;
  private Table<String, ByteString> statefulServiceConfigTable;
  private Table<String, TransactionInfo> transactionInfoTable;
  private InterSCMGrpcService service;

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    metadataStore = new SCMMetadataStoreImpl(conf);
    statefulServiceConfigTable = metadataStore.getStatefulServiceConfigTable();
    transactionInfoTable = metadataStore.getTransactionInfoTable();

    StorageContainerManager scm = mock(StorageContainerManager.class);
    BlockManager blockManager = mock(BlockManager.class);
    Clock clock = mock(Clock.class);
    when(clock.millis()).thenAnswer(invocation -> clockMillis.get());
    when(scm.getConfiguration()).thenReturn(conf);
    when(scm.getScmMetadataStore()).thenReturn(metadataStore);
    when(scm.getSystemClock()).thenReturn(clock);
    when(scm.getScmBlockManager()).thenReturn(blockManager);
    when(scm.getPerfMetrics()).thenReturn(mock(SCMPerformanceMetrics.class));
    when(blockManager.getDeletedBlockLog())
        .thenReturn(mock(DeletedBlockLogImpl.class));

    transactionBuffer = new SCMHADBTransactionBufferImpl(scm);
    SCMHAManager haManager = mock(SCMHAManager.class);
    when(haManager.asSCMHADBTransactionBuffer()).thenReturn(transactionBuffer);
    when(scm.getScmHAManager()).thenReturn(haManager);
    when(scm.getClusterId()).thenReturn("clusterId");
    service = new InterSCMGrpcService(scm);
  }

  @AfterEach
  void cleanup() throws Exception {
    if (transactionBuffer != null) {
      transactionBuffer.close();
    }
    if (metadataStore != null) {
      metadataStore.stop();
    }
  }

  @Test
  void downloadDoesNotFlushDuringApply() throws Exception {
    transactionBuffer.updateLatestTrxInfo(TRX_INFO_T4);
    transactionBuffer.flush();

    transactionBuffer.beginApplyingTransaction();
    try {
      transactionBuffer.addToBuffer(statefulServiceConfigTable, "key",
          ByteString.copyFromUtf8("value"));

      clockMillis.incrementAndGet();
      StreamObserver<CopyDBCheckpointResponseProto> observer = newObserver();
      service.download(checkpointRequest(), observer);

      verify(observer).onCompleted();
      verify(observer, never()).onError(any());
      assertThat(transactionInfoTable.get(TRANSACTION_INFO_KEY))
          .isEqualTo(TRX_INFO_T4);
      assertThat(statefulServiceConfigTable.get("key")).isNull();
    } finally {
      transactionBuffer.updateLatestTrxInfo(TRX_INFO_T5);
      transactionBuffer.endApplyingTransaction();
    }

    clockMillis.incrementAndGet();
    StreamObserver<CopyDBCheckpointResponseProto> observer = newObserver();
    service.download(checkpointRequest(), observer);

    verify(observer).onCompleted();
    verify(observer, never()).onError(any());
    assertThat(transactionInfoTable.get(TRANSACTION_INFO_KEY))
        .isEqualTo(TRX_INFO_T5);
    assertThat(statefulServiceConfigTable.get("key"))
        .isEqualTo(ByteString.copyFromUtf8("value"));
  }

  private static CopyDBCheckpointRequestProto checkpointRequest() {
    return CopyDBCheckpointRequestProto.newBuilder().setFlush(true).build();
  }

  @SuppressWarnings("unchecked")
  private static StreamObserver<CopyDBCheckpointResponseProto> newObserver() {
    return mock(StreamObserver.class);
  }
}
