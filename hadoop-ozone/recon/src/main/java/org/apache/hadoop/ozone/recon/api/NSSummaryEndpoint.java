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

import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/nssummary")
@Produces(MediaType.APPLICATION_JSON)
public class NSSummaryEndpoint {
  @Inject
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @Inject
  private ReconOMMetadataManager omMetadataManager;
/*
  @GET
  @Path("/basic")
  public Response getBasicInfoById(
          @QueryParam("entityType") String type,
          @QueryParam("objectId") long objectId) {

  }*/

  // "vol/bucket/" bucket
  // "vol/"
  // getVolumeKey
  // getBucketKey

  // /vol/bucket/dir1/dir2/dir3
}
