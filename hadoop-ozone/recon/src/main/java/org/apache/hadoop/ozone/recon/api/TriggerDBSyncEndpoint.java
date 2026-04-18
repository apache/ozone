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
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;

/**
 * Admin-only endpoint to manually trigger DB sync operations between Recon
 * and its upstream sources (OM and SCM).
 *
 * <p>Available endpoints:
 * <ul>
 *   <li>{@code GET  /api/v1/triggerdbsync/om}  — triggers full OM DB sync</li>
 *   <li>{@code POST /api/v1/triggerdbsync/scm} — triggers targeted SCM
 *       container sync (four-pass incremental: add missing CLOSED/OPEN/
 *       QUASI_CLOSED containers, correct stale OPEN state, retire DELETED
 *       containers)</li>
 * </ul>
 */
@Path("/triggerdbsync")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class TriggerDBSyncEndpoint {

  private final OzoneManagerServiceProvider ozoneManagerServiceProvider;
  private final ReconStorageContainerManagerFacade reconScm;

  @Inject
  public TriggerDBSyncEndpoint(
      OzoneManagerServiceProvider ozoneManagerServiceProvider,
      ReconStorageContainerManagerFacade reconScm) {
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
    this.reconScm = reconScm;
  }

  /**
   * Triggers an immediate full OM DB sync between Recon and the Ozone Manager.
   *
   * @return {@code true} if the sync was initiated successfully.
   */
  @GET
  @Path("om")
  public Response triggerOMDBSync() {
    boolean isSuccess =
        ozoneManagerServiceProvider.triggerSyncDataFromOMImmediately();
    return Response.ok(isSuccess).build();
  }

  /**
   * Triggers an immediate targeted SCM container sync.
   *
   * <p>Runs the four-pass incremental sync unconditionally (bypassing the
   * periodic drift-based decision):
   * <ol>
   *   <li>Pass 1 (CLOSED): adds missing CLOSED containers and corrects
   *       containers stuck as OPEN or CLOSING in Recon.</li>
   *   <li>Pass 2 (OPEN): adds OPEN containers that Recon never received
   *       (e.g., created while Recon was down).</li>
   *   <li>Pass 3 (QUASI_CLOSED): adds QUASI_CLOSED containers absent from
   *       Recon.</li>
   *   <li>Pass 4 (DELETED retirement): transitions containers that SCM has
   *       marked DELETED from their current Recon state (CLOSED/QUASI_CLOSED)
   *       forward to DELETED in Recon's metadata store.</li>
   * </ol>
   *
   * <p>This endpoint is useful for immediately resolving known discrepancies
   * without waiting for the next periodic sync cycle (default: every 1h).
   * For large-scale drift (hundreds of containers), consider triggering a
   * full SCM DB snapshot sync instead via the Recon admin REST API.
   *
   * @return {@code true} if all four passes completed without fatal errors,
   *         {@code false} if one or more passes encountered errors (partial
   *         sync may have occurred; check Recon logs for details).
   */
  @POST
  @Path("scm")
  public Response triggerSCMContainerSync() {
    boolean isSuccess = reconScm.syncWithSCMContainerInfo();
    return Response.ok(isSuccess).build();
  }
}
