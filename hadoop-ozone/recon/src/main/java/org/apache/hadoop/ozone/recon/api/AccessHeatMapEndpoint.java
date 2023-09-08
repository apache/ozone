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

import org.apache.hadoop.ozone.recon.api.types.EntityReadAccessHeatMapResponse;
import org.apache.hadoop.ozone.recon.api.types.FeatureProvider;
import org.apache.hadoop.ozone.recon.heatmap.HeatMapServiceImpl;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_ACCESS_METADATA_START_DATE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_ENTITY_PATH;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_ENTITY_TYPE;


/**
 * Endpoint for querying access metadata from HeatMapProvider interface
 * to generate heatmap in Recon.
 */
@Path("/heatmap")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
@InternalOnly(feature = "Heatmap", description = "Heatmap feature has " +
    "dependency on heatmap provider service component implementation.")
public class AccessHeatMapEndpoint {

  private HeatMapServiceImpl heatMapService;

  @Inject
  public AccessHeatMapEndpoint(HeatMapServiceImpl heatMapService) {
    this.heatMapService = heatMapService;
  }

  /**
   * Return the top 100 prefixes or paths
   * in tree nested structure with root as
   * "/" and based on top 100 paths, response
   * will be structured as tree starting
   * with volume, buckets under that volume,
   * then directories, subdirectories and paths
   * under that bucket.
   * E.g. -------->>
   * vol1                           vol2
   * - bucket1                      - bucket2
   * - dir1/dir2/key1               - dir4/dir1/key1
   * - dir1/dir2/key2               - dir4/dir5/key2
   * - dir1/dir3/key1               - dir5/dir3/key1
   *
   * @return {@link Response}
   */
  @GET
  @Path("/readaccess")
  public Response getReadAccessMetaData(
      @QueryParam(RECON_ENTITY_PATH) String path,
      @DefaultValue("key") @QueryParam(RECON_ENTITY_TYPE) String entityType,
      @DefaultValue("24H") @QueryParam(RECON_ACCESS_METADATA_START_DATE)
      String startDate) {
    checkIfHeatMapFeatureIsEnabled();
    EntityReadAccessHeatMapResponse entityReadAccessHeatMapResponse = null;
    try {
      entityReadAccessHeatMapResponse =
          heatMapService.retrieveData(path, entityType, startDate);
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(entityReadAccessHeatMapResponse).build();
  }

  private static void checkIfHeatMapFeatureIsEnabled() {
    FeatureProvider.Feature heatMapFeature = null;
    List<FeatureProvider.Feature> allDisabledFeatures =
        FeatureProvider.getAllDisabledFeatures();
    for (FeatureProvider.Feature feature : allDisabledFeatures) {
      if ("HeatMap".equals(feature.getFeatureName())) {
        heatMapFeature = feature;
        break;
      }
    }
    if (null != heatMapFeature) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    }
  }
}
