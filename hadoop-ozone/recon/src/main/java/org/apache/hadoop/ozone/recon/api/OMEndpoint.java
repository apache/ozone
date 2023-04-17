/*
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

import org.apache.hadoop.ozone.recon.api.types.VolumeMetadata;
import org.apache.hadoop.ozone.recon.api.types.VolumesResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

/**
 * Endpoint to fetch details about volume.
 */
@Path("/om")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class OMEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMEndpoint.class);

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  @Inject
  public OMEndpoint(ReconOMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
  }

  @GET
  @Path("/volumes")
  public Response getVolumes() throws IOException {
    List<VolumeMetadata> volumes = omMetadataManager.getAllVolumes();
    VolumesResponse volumesResponse =
        new VolumesResponse(volumes.size(), volumes);
    return Response.ok(volumesResponse).build();
  }

}
