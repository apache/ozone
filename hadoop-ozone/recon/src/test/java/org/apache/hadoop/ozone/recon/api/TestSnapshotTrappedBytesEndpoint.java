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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.recon.api.types.SnapshotTrappedBytesJob;
import org.apache.hadoop.ozone.recon.api.types.SnapshotTrappedBytesJob.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SnapshotTrappedBytesEndpoint} and {@link SnapshotTrappedBytesJobManager}.
 */
public class TestSnapshotTrappedBytesEndpoint {

  private OzoneManagerProtocol omClient;
  private final List<Runnable> submittedTasks = new ArrayList<>();
  private SnapshotTrappedBytesEndpoint endpoint;

  @BeforeEach
  public void setUp() {
    omClient = mock(OzoneManagerProtocol.class);
    // Capture the tasks instead of running them, so each test controls when a job runs.
    ExecutorService executor = mock(ExecutorService.class);
    when(executor.submit(any(Runnable.class))).then(invocation -> {
      submittedTasks.add(invocation.getArgument(0));
      return null;
    });
    endpoint = new SnapshotTrappedBytesEndpoint(new SnapshotTrappedBytesJobManager(omClient, executor));
  }

  @Test
  public void testJobCompletes() throws Exception {
    when(omClient.getBucketDeletedBytes("vol1/bucket1")).thenReturn(BucketDeletedBytes.newBuilder()
        .setSnapshotTrappedBytes(300).setPurgeableBytes(600)
        .setSnapshotTrappedKeys(1).setPurgeableKeys(2)
        .setSnapshotTrappedDirs(3).setPurgeableDirs(4)
        .build());

    Response response = endpoint.submitJob("vol1", "bucket1");
    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    SnapshotTrappedBytesJob job = (SnapshotTrappedBytesJob) response.getEntity();
    assertEquals(JobStatus.QUEUED, job.getStatus());
    assertNull(job.getResult());

    runSubmittedTasks();

    SnapshotTrappedBytesJob polled = (SnapshotTrappedBytesJob) endpoint.getJob(job.getJobId()).getEntity();
    assertEquals(JobStatus.COMPLETED, polled.getStatus());
    assertEquals(300, polled.getResult().getSnapshotTrappedBytes());
    assertEquals(600, polled.getResult().getPurgeableBytes());
    assertEquals(1, polled.getResult().getSnapshotTrappedKeys());
    assertEquals(2, polled.getResult().getPurgeableKeys());
    assertEquals(3, polled.getResult().getSnapshotTrappedDirs());
    assertEquals(4, polled.getResult().getPurgeableDirs());

    List<?> jobs = (List<?>) endpoint.listJobs().getEntity();
    assertEquals(1, jobs.size());
  }

  @Test
  public void testJobFailsWhenOmRejectsRequest() throws Exception {
    when(omClient.getBucketDeletedBytes("vol1/bucket1")).thenThrow(new OMException(
        "Only Ozone admins are allowed to get bucket deleted bytes.", OMException.ResultCodes.PERMISSION_DENIED));

    SnapshotTrappedBytesJob job = (SnapshotTrappedBytesJob) endpoint.submitJob("vol1", "bucket1").getEntity();
    runSubmittedTasks();

    assertEquals(JobStatus.FAILED, job.getStatus());
    assertEquals("Only Ozone admins are allowed to get bucket deleted bytes.", job.getErrorMessage());
    assertNull(job.getResult());
  }

  @Test
  public void testActiveJobIsReusedForSameBucket() throws Exception {
    SnapshotTrappedBytesJob first = (SnapshotTrappedBytesJob) endpoint.submitJob("vol1", "bucket1").getEntity();
    SnapshotTrappedBytesJob second = (SnapshotTrappedBytesJob) endpoint.submitJob("vol1", "bucket1").getEntity();
    assertSame(first, second);
    assertEquals(1, submittedTasks.size());

    // Once the job finished, a new request starts a fresh calculation.
    when(omClient.getBucketDeletedBytes("vol1/bucket1")).thenReturn(BucketDeletedBytes.newBuilder().build());
    runSubmittedTasks();
    SnapshotTrappedBytesJob third = (SnapshotTrappedBytesJob) endpoint.submitJob("vol1", "bucket1").getEntity();
    assertEquals(JobStatus.QUEUED, third.getStatus());
    assertEquals(1, submittedTasks.size());
  }

  @Test
  public void testTooManyActiveJobs() {
    for (int i = 0; i < SnapshotTrappedBytesJobManager.MAX_ACTIVE_JOBS; i++) {
      assertEquals(Response.Status.ACCEPTED.getStatusCode(), endpoint.submitJob("vol1", "bucket" + i).getStatus());
    }
    Response response = endpoint.submitJob("vol1", "oneTooMany");
    assertEquals(Response.Status.TOO_MANY_REQUESTS.getStatusCode(), response.getStatus());
    assertEquals(SnapshotTrappedBytesJobManager.MAX_ACTIVE_JOBS, submittedTasks.size());
  }

  @Test
  public void testInvalidRequests() throws Exception {
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), endpoint.submitJob(null, "bucket1").getStatus());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), endpoint.submitJob("vol1", " ").getStatus());
    Response badPath = endpoint.submitJob("vol1", "bucket1/key");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), badPath.getStatus());
    assertEquals("Bad Request", ((Map<?, ?>) badPath.getEntity()).get("error"));
    assertEquals(0, submittedTasks.size());
    verify(omClient, never()).getBucketDeletedBytes(any());

    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), endpoint.getJob("unknown").getStatus());
  }

  private void runSubmittedTasks() {
    List<Runnable> tasks = new ArrayList<>(submittedTasks);
    submittedTasks.clear();
    tasks.forEach(Runnable::run);
  }
}
