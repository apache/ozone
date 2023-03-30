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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.recon.api.types.OpenKeyInsightInfoResp;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_FETCH_COUNT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;

/**
 * Endpoint to get following key level info under OM DB Insight page of Recon.
 * 1. Number of open keys for Legacy/OBS buckets.
 * 2. Number of open files for FSO buckets.
 * 3. Amount of data mapped to open keys and open files.
 * 4. Number of pending delete keys in legacy/OBS buckets and pending
 * delete files in FSO buckets.
 * 5. Amount of data mapped to pending delete keys in legacy/OBS buckets and
 * pending delete files in FSO buckets.
 */
@Path("/omdbinsight")
@Produces(MediaType.APPLICATION_JSON)
public class OMDBInsightEndpoint {

  private OzoneManagerServiceProvider ozoneManagerServiceProvider;

  @Inject
  public OMDBInsightEndpoint(
      OzoneManagerServiceProvider ozoneManagerServiceProvider) {
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
  }

  @GET
  @Path("openkeyinfo")
  public Response getOpenKeyInfo(
      @DefaultValue(DEFAULT_FETCH_COUNT) @QueryParam(RECON_QUERY_LIMIT)
      int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
      String prevKeyPrefix) {
    OpenKeyInsightInfoResp openKeyInsightInfoResp =
        ozoneManagerServiceProvider.retrieveOpenKeyInfo(limit, prevKeyPrefix);
    return Response.ok(openKeyInsightInfoResp).build();
  }
}
