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

package org.apache.hadoop.ozone.om.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Class to unit test SnapshotDeletingService.
 */
@ExtendWith(MockitoExtension.class)
public class TestSnapshotDeletingService {
  @Mock
  private OzoneManager ozoneManager;
  @Mock
  private KeyManagerImpl keyManager;
  @Mock
  private OmSnapshotManager omSnapshotManager;
  @Mock
  private SnapshotChainManager chainManager;
  @Mock
  private OmMetadataManagerImpl omMetadataManager;
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private final long sdsRunInterval = Duration.ofMillis(1000).toMillis();
  private final  long sdsServiceTimeout = Duration.ofSeconds(10).toMillis();

  private static Stream<Arguments> testCasesForIgnoreSnapshotGc() throws IOException {
    SnapshotInfo flushedSnapshot = SnapshotInfo.newBuilder().setSstFiltered(true)
        .setLastTransactionInfo(TransactionInfo.valueOf(1, 1).toByteString())
        .setName("snap1").build();
    SnapshotInfo unFlushedSnapshot = SnapshotInfo.newBuilder().setSstFiltered(false).setName("snap1")
        .setLastTransactionInfo(TransactionInfo.valueOf(0, 0).toByteString()).build();
    return Stream.of(
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFlushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(flushedSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true));
  }

  @ParameterizedTest
  @MethodSource("testCasesForIgnoreSnapshotGc")
  public void testProcessSnapshotLogicInSDS(SnapshotInfo snapshotInfo,
                                            SnapshotInfo.SnapshotStatus status,
                                            boolean expectedOutcome)
      throws IOException {
    Mockito.when(omMetadataManager.getSnapshotChainManager()).thenReturn(chainManager);
    Mockito.when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
    Mockito.when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    Mockito.when(ozoneManager.getConfiguration()).thenReturn(conf);
    if (status == SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED) {
      Table<String, TransactionInfo> transactionInfoTable = Mockito.mock(Table.class);
      Mockito.when(omMetadataManager.getTransactionInfoTable()).thenReturn(transactionInfoTable);
      Mockito.when(transactionInfoTable.getSkipCache(Mockito.anyString()))
          .thenReturn(TransactionInfo.valueOf(1, 1));
    }

    SnapshotDeletingService snapshotDeletingService =
        new SnapshotDeletingService(sdsRunInterval, sdsServiceTimeout, ozoneManager);

    snapshotInfo.setSnapshotStatus(status);
    assertEquals(expectedOutcome, snapshotDeletingService.shouldIgnoreSnapshot(snapshotInfo));
  }
}
