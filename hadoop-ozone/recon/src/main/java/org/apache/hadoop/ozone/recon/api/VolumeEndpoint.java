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

import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.VolumesResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;

/**
 * Endpoint to fetch details about volumes.
 */
@Path("/volumes")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class VolumeEndpoint {

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  @Inject
  public VolumeEndpoint(ReconOMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
  }

  @GET
  public Response getVolumes(
      @DefaultValue(DEFAULT_FETCH_COUNT)
      @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(StringUtils.EMPTY)
      @QueryParam(RECON_QUERY_PREVKEY) String prevKey
  ) throws IOException {
    List<OmVolumeArgs> volumes = omMetadataManager.listVolumes(
        prevKey, limit);
    List<VolumeObjectDBInfo> volumeMetadata = volumes.stream()
        .map(VolumeObjectDBInfo::new).collect(Collectors.toList());
    VolumesResponse volumesResponse =
        new VolumesResponse(volumes.size(), volumeMetadata);
    return Response.ok(volumesResponse).build();
  }
}
