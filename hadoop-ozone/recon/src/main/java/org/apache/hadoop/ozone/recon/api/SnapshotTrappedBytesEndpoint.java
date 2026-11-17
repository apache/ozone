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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.recon.api.types.SnapshotTrappedBytesJob;

/**
 * Endpoint to calculate, on demand, how much of the data pending deletion in a bucket is trapped by snapshots
 * and how much is purgeable. The calculation is delegated to OM and runs asynchronously:
 * <ol>
 *   <li>{@code POST /snapshotTrappedBytes?volume=vol1&bucket=bucket1} submits a job and returns its id.</li>
 *   <li>{@code GET /snapshotTrappedBytes/{jobId}} is polled until the job is COMPLETED or FAILED.</li>
 *   <li>{@code GET /snapshotTrappedBytes} lists the queued, running and recently finished jobs.</li>
 * </ol>
 * Recon must be an OM admin for OM to accept the request.
 */
@Path("/snapshotTrappedBytes")
@Produces(MediaType.APPLICATION_JSON)
public class SnapshotTrappedBytesEndpoint {

  private final SnapshotTrappedBytesJobManager jobManager;

  @Inject
  public SnapshotTrappedBytesEndpoint(SnapshotTrappedBytesJobManager jobManager) {
    this.jobManager = jobManager;
  }

  @GET
  public Response listJobs() {
    return Response.ok(jobManager.getJobs()).build();
  }

  /**
   * Submits a snapshot-trapped bytes calculation for a bucket.
   * If one is already queued or running for the bucket, that job is returned instead.
   *
   * Example response:
   *   {
   *     "jobId": "6f1c3c4e-...",
   *     "volumeName": "vol1",
   *     "bucketName": "bucket1",
   *     "status": "QUEUED",
   *     "submittedAt": 1730000000000,
   *     "startedAt": 0,
   *     "completedAt": 0,
   *     "errorMessage": null,
   *     "result": null
   *   }
   */
  @POST
  public Response submitJob(@QueryParam("volume") String volume, @QueryParam("bucket") String bucket) {
    if (!isValidName(volume) || !isValidName(bucket)) {
      return errorResponse(Response.Status.BAD_REQUEST, "Bad Request",
          "volume and bucket query parameters are required and must not contain '" + OM_KEY_PREFIX + "'");
    }
    try {
      return Response.status(Response.Status.ACCEPTED).entity(jobManager.submitJob(volume, bucket)).build();
    } catch (IllegalStateException e) {
      return errorResponse(Response.Status.TOO_MANY_REQUESTS, "Too Many Requests", e.getMessage());
    }
  }

  /**
   * Returns a snapshot-trapped bytes job. When the status is COMPLETED, "result" holds:
   *   {
   *     "snapshotTrappedBytes": 1048576,
   *     "purgeableBytes": 2097152,
   *     "snapshotTrappedKeys": 1,
   *     "purgeableKeys": 2,
   *     "snapshotTrappedDirs": 0,
   *     "purgeableDirs": 1
   *   }
   * When the status is FAILED, "errorMessage" holds the reason.
   */
  @GET
  @Path("/{jobId}")
  public Response getJob(@PathParam("jobId") String jobId) {
    SnapshotTrappedBytesJob job = jobManager.getJob(jobId);
    if (job == null) {
      return errorResponse(Response.Status.NOT_FOUND, "Not Found", "Job not found or expired: " + jobId);
    }
    return Response.ok(job).build();
  }

  private static boolean isValidName(String name) {
    return StringUtils.isNotBlank(name) && !name.contains(OM_KEY_PREFIX);
  }

  private static Response errorResponse(Response.Status status, String error, String message) {
    Map<String, String> body = new HashMap<>();
    body.put("error", error);
    body.put("message", message);
    return Response.status(status).entity(body).type(MediaType.APPLICATION_JSON).build();
  }
}
