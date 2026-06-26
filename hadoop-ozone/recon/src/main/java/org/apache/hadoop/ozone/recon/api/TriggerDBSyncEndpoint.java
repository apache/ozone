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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;

/**
 * Endpoint to trigger the OM DB sync between Recon and OM.
 */
@Path("/triggerdbsync")
@Produces(MediaType.APPLICATION_JSON)
public class TriggerDBSyncEndpoint {

  private OzoneManagerServiceProvider ozoneManagerServiceProvider;
  private ReconStorageContainerManagerFacade reconScm;

  @Inject
  public TriggerDBSyncEndpoint(
      OzoneManagerServiceProvider ozoneManagerServiceProvider,
      OzoneStorageContainerManager reconScm) {
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
    this.reconScm = (ReconStorageContainerManagerFacade) reconScm;
  }

  @GET
  @Path("om")
  public Response triggerOMDBSync() {
    boolean isSuccess =
        ozoneManagerServiceProvider.triggerSyncDataFromOMImmediately();
    return Response.ok(isSuccess).build();
  }

  @POST
  @Path("scm/snapshot")
  public Response triggerSCMDBSnapshotSync() {
    ReconStorageContainerManagerFacade.ScmDbSnapshotTriggerResponse response =
        reconScm.triggerScmDbSnapshotSync();
    return response.isAccepted()
        ? Response.accepted(response).build()
        : Response.status(Response.Status.CONFLICT).entity(response).build();
  }

  @GET
  @Path("scm/snapshot/status")
  public Response getSCMDBSnapshotSyncStatus() {
    return Response.ok(reconScm.getScmDbSnapshotSyncStatus()).build();
  }

  @POST
  @Path("scm/snapshot/cancel")
  public Response cancelSCMDBSnapshotSync() {
    ReconStorageContainerManagerFacade.ScmDbSnapshotCancelResponse response =
        reconScm.cancelScmDbSnapshotSync();
    return response.isCancelled()
        ? Response.ok(response).build()
        : Response.status(Response.Status.CONFLICT).entity(response).build();
  }
}
