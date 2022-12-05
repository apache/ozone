/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Endpoint to trigger the OM DB sync between Recon and OM.
 */
@Path("/triggerdbsync")
@Produces(MediaType.APPLICATION_JSON)
public class TriggerDBSyncEndpoint {

  private OzoneManagerServiceProvider ozoneManagerServiceProvider;

  @Inject
  public TriggerDBSyncEndpoint(
      OzoneManagerServiceProvider ozoneManagerServiceProvider) {
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
  }

  @GET
  @Path("om")
  public Response triggerOMDBSync() {
    boolean isSuccess =
        ozoneManagerServiceProvider.triggerSyncDataFromOMImmediately();
    return Response.ok(isSuccess).build();
  }
}
