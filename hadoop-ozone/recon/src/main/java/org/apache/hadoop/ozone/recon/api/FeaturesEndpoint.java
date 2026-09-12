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

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.FeatureProvider;
import org.apache.hadoop.ozone.recon.heatmap.HeatMapServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Endpoint for APIs related to features in Recon.
 * API: "api/v1/features/disabledFeatures"
 *    Disabled features may represent or fall under below categories:
 *      1. Implementation Not Available
 *      2. Beta Phase
 */
@Path("/features")
@Produces(MediaType.APPLICATION_JSON)
public class FeaturesEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(HeatMapServiceImpl.class);
  private final OzoneConfiguration ozoneConfiguration;

  @Inject
  public FeaturesEndpoint(OzoneConfiguration ozoneConfiguration) {
    this.ozoneConfiguration = ozoneConfiguration;
  }

  /**
   * This API returns the list of all disabled features in Recon.
   *
   * @return {@link Response}
   */
  @GET
  @Path("/disabledFeatures")
  public Response getDisabledFeatures() {
    List<FeatureProvider.Feature> allDisabledFeatures;
    try {
      allDisabledFeatures = FeatureProvider.getAllDisabledFeatures();
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    LOG.info("List of disabled features in Recon: {}",
        allDisabledFeatures.toString());
    return Response.ok(allDisabledFeatures).build();
  }
}
