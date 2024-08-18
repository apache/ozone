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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.service;


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
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

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
  @Mock
  private ScmBlockLocationProtocol scmClient;
  private final OzoneConfiguration conf = new OzoneConfiguration();;
  private final long sdsRunInterval = Duration.ofMillis(1000).toMillis();
  private final  long sdsServiceTimeout = Duration.ofSeconds(10).toMillis();


  private static Stream<Arguments> testCasesForIgnoreSnapshotGc() {
    SnapshotInfo filteredSnapshot = SnapshotInfo.newBuilder().setSstFiltered(true).setName("snap1").build();
    SnapshotInfo unFilteredSnapshot = SnapshotInfo.newBuilder().setSstFiltered(false).setName("snap1").build();
    return Stream.of(
        Arguments.of(filteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(filteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(unFilteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFilteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(filteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFilteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED, false),
        Arguments.of(unFilteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true),
        Arguments.of(filteredSnapshot, SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE, true));
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

    SnapshotDeletingService snapshotDeletingService =
        new SnapshotDeletingService(sdsRunInterval, sdsServiceTimeout, ozoneManager, scmClient);

    snapshotInfo.setSnapshotStatus(status);
    assertEquals(expectedOutcome, snapshotDeletingService.shouldIgnoreSnapshot(snapshotInfo));
  }
}
