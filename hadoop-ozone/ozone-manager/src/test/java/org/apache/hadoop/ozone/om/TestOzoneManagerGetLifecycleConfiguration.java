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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for audit reporting of {@link OzoneManager#getLifecycleConfiguration}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneManagerGetLifecycleConfiguration {

  private static final String REQUESTED_VOLUME = "requestedVolume";
  private static final String REQUESTED_BUCKET = "requestedBucket";
  private static final String REAL_VOLUME = "realVolume";
  private static final String REAL_BUCKET = "realBucket";

  private OmTestManagers omTestManagers;
  private OzoneManager om;

  private OzoneManager omSpy;

  @BeforeAll
  void setup(@TempDir File folder) throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.toString());
    omTestManagers = new OmTestManagers(conf);
    om = omTestManagers.getOzoneManager();
  }

  @AfterAll
  void cleanup() {
    if (omTestManagers != null) {
      omTestManagers.stop();
    }
  }

  @BeforeEach
  void init() throws Exception {
    omSpy = spy(om);
    HddsWhiteboxTestUtils.setInternalState(omSpy, "omMetadataReader", mock(OmMetadataReader.class));

    doReturn(new ResolvedBucket(REQUESTED_VOLUME, REQUESTED_BUCKET, REAL_VOLUME, REAL_BUCKET, "owner", null))
        .when(omSpy).resolveBucketLink(Pair.of(REQUESTED_VOLUME, REQUESTED_BUCKET));

    final AuditMessage mockAuditMessage = mock(AuditMessage.class);
    when(mockAuditMessage.getOp()).thenReturn(OMAction.GET_LIFECYCLE_CONFIGURATION.getAction());
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForSuccess(any(), anyMap());
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));
  }

  @Test
  void testMissingConfigurationIsAuditedAsCompletedRead() throws Exception {
    // Nothing was written to the lifecycle configuration table, so the read reports not-found.
    OMException ex = assertThrows(OMException.class,
        () -> omSpy.getLifecycleConfiguration(REQUESTED_VOLUME, REQUESTED_BUCKET));

    assertEquals(OMException.ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND, ex.getResult());
    verify(omSpy, never()).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));
    verify(omSpy).buildAuditMessageForSuccess(eq(OMAction.GET_LIFECYCLE_CONFIGURATION), anyMap());
  }

  @Test
  void testGenuineFailureIsAuditedAsReadFailure() throws Exception {
    OMException failure = new OMException("read failure", OMException.ResultCodes.INTERNAL_ERROR);
    OMMetadataManager metadataManagerSpy = spy(om.getMetadataManager());
    doThrow(failure).when(metadataManagerSpy).getLifecycleConfiguration(REAL_VOLUME, REAL_BUCKET);
    HddsWhiteboxTestUtils.setInternalState(omSpy, "metadataManager", metadataManagerSpy);

    OMException ex = assertThrows(OMException.class,
        () -> omSpy.getLifecycleConfiguration(REQUESTED_VOLUME, REQUESTED_BUCKET));

    assertEquals(OMException.ResultCodes.INTERNAL_ERROR, ex.getResult());
    verify(omSpy).buildAuditMessageForFailure(eq(OMAction.GET_LIFECYCLE_CONFIGURATION), anyMap(), eq(failure));
    verify(omSpy, never()).buildAuditMessageForSuccess(any(), anyMap());
  }
}
