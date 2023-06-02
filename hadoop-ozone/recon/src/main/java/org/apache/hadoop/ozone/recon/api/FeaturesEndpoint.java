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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.Feature;
import org.apache.hadoop.ozone.recon.heatmap.HeatMapServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;


/**
 * Endpoint for listing of disabled feature list in Recon.
 * Disabled features may represent or fall under below categories:
 *  1. Implementation Not Available
 *  2. Beta Phase
 */
@Path("/features")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class FeaturesEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(HeatMapServiceImpl.class);
  private final OzoneConfiguration ozoneConfiguration;

  @Inject
  public FeaturesEndpoint(OzoneConfiguration ozoneConfiguration) {
    this.ozoneConfiguration = ozoneConfiguration;
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
  @Path("/disabledFeatures")
  public Response getDisabledFeatures() {
    List<Feature> allDisabledFeatures;
    try {
      allDisabledFeatures = Feature.getAllDisabledFeatures();
    } catch (Exception ex) {
      throw new WebApplicationException(ex,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(allDisabledFeatures).build();
  }
}
