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

package org.apache.hadoop.ozone.recon.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade.ScmDbSnapshotCancelResponse;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade.ScmDbSnapshotStatusResponse;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade.ScmDbSnapshotSyncPhase;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade.ScmDbSnapshotSyncStatus;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade.ScmDbSnapshotTriggerResponse;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.jupiter.api.Test;

/**
 * Test class for OMSyncEndpoint.
 */
public class TestTriggerDBSyncEndpoint {

  @Test
  public void testTriggerDBSyncEndpointWithOM() {
    OzoneManagerServiceProviderImpl omProvider = mock(OzoneManagerServiceProviderImpl.class);
    when(omProvider.triggerSyncDataFromOMImmediately()).thenReturn(true);
    TriggerDBSyncEndpoint triggerDBSyncEndpoint =
        new TriggerDBSyncEndpoint(omProvider, mock(ReconStorageContainerManagerFacade.class));
    Response response = triggerDBSyncEndpoint.triggerOMDBSync();
    assertEquals(200, response.getStatus());
    assertEquals(true, response.getEntity());
    verify(omProvider).triggerSyncDataFromOMImmediately();
  }

  @Test
  public void testTriggerSCMDBSnapshotSyncAccepted() {
    OzoneManagerServiceProviderImpl omProvider =
        mock(OzoneManagerServiceProviderImpl.class);
    ReconStorageContainerManagerFacade reconScm =
        mock(ReconStorageContainerManagerFacade.class);
    when(reconScm.triggerScmDbSnapshotSync()).thenReturn(
        new ScmDbSnapshotTriggerResponse(true,
            ScmDbSnapshotSyncStatus.IN_PROGRESS,
            "SCM DB snapshot sync started."));

    TriggerDBSyncEndpoint endpoint =
        new TriggerDBSyncEndpoint(omProvider, reconScm);
    Response response = endpoint.triggerSCMDBSnapshotSync();

    assertEquals(202, response.getStatus());
    ScmDbSnapshotTriggerResponse entity =
        (ScmDbSnapshotTriggerResponse) response.getEntity();
    assertTrue(entity.isAccepted());
    assertEquals(ScmDbSnapshotSyncStatus.IN_PROGRESS, entity.getStatus());
  }

  @Test
  public void testTriggerSCMDBSnapshotSyncConflict() {
    OzoneManagerServiceProviderImpl omProvider =
        mock(OzoneManagerServiceProviderImpl.class);
    ReconStorageContainerManagerFacade reconScm =
        mock(ReconStorageContainerManagerFacade.class);
    when(reconScm.triggerScmDbSnapshotSync()).thenReturn(
        new ScmDbSnapshotTriggerResponse(false,
            ScmDbSnapshotSyncStatus.IN_PROGRESS,
            "SCM DB sync is already running."));

    TriggerDBSyncEndpoint endpoint =
        new TriggerDBSyncEndpoint(omProvider, reconScm);
    Response response = endpoint.triggerSCMDBSnapshotSync();

    assertEquals(409, response.getStatus());
    ScmDbSnapshotTriggerResponse entity =
        (ScmDbSnapshotTriggerResponse) response.getEntity();
    assertFalse(entity.isAccepted());
  }

  @Test
  public void testGetSCMDBSnapshotSyncStatus() {
    OzoneManagerServiceProviderImpl omProvider =
        mock(OzoneManagerServiceProviderImpl.class);
    ReconStorageContainerManagerFacade reconScm =
        mock(ReconStorageContainerManagerFacade.class);
    when(reconScm.getScmDbSnapshotSyncStatus()).thenReturn(
        new ScmDbSnapshotStatusResponse(
            ScmDbSnapshotSyncStatus.IN_PROGRESS,
            ScmDbSnapshotSyncPhase.DOWNLOADING_CHECKPOINT,
            1L, 0L, true, null));

    TriggerDBSyncEndpoint endpoint =
        new TriggerDBSyncEndpoint(omProvider, reconScm);
    Response response = endpoint.getSCMDBSnapshotSyncStatus();

    assertEquals(200, response.getStatus());
    ScmDbSnapshotStatusResponse entity =
        (ScmDbSnapshotStatusResponse) response.getEntity();
    assertEquals(ScmDbSnapshotSyncStatus.IN_PROGRESS, entity.getStatus());
    assertEquals(ScmDbSnapshotSyncPhase.DOWNLOADING_CHECKPOINT,
        entity.getPhase());
    assertTrue(entity.isCancelAllowed());
  }

  @Test
  public void testCancelSCMDBSnapshotSync() {
    OzoneManagerServiceProviderImpl omProvider =
        mock(OzoneManagerServiceProviderImpl.class);
    ReconStorageContainerManagerFacade reconScm =
        mock(ReconStorageContainerManagerFacade.class);
    when(reconScm.cancelScmDbSnapshotSync()).thenReturn(
        new ScmDbSnapshotCancelResponse(true,
            ScmDbSnapshotSyncStatus.CANCELLED,
            ScmDbSnapshotSyncPhase.CANCELLED,
            "SCM DB snapshot sync cancelled."));

    TriggerDBSyncEndpoint endpoint =
        new TriggerDBSyncEndpoint(omProvider, reconScm);
    Response response = endpoint.cancelSCMDBSnapshotSync();

    assertEquals(200, response.getStatus());
    ScmDbSnapshotCancelResponse entity =
        (ScmDbSnapshotCancelResponse) response.getEntity();
    assertTrue(entity.isCancelled());
    assertEquals(ScmDbSnapshotSyncStatus.CANCELLED, entity.getStatus());
  }
}
