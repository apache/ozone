/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for ReconContextEndpoint.
 */
public class TestReconContextEndpoint {

  @TempDir
  private Path temporaryFolder;

  private ReconContextEndpoint endpoint;
  private ReconContext reconContext;

  @BeforeEach
  public void setUp() throws Exception {
    // Initialize Ozone Configuration
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.metadata.dirs", temporaryFolder.toAbsolutePath().toString());
    conf.set("ozone.scm.names", "localhost");
    ReconUtils reconUtils = new ReconUtils();
    reconContext = new ReconContext(conf, reconUtils);
    endpoint = new ReconContextEndpoint(reconContext);
  }

  @Test
  public void testGetReconContextStatus() {
    Response response = endpoint.getReconContextStatus();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertNotNull(response.getEntity());

    // Additional checks on the structure/content
    Map<String, Object> contextData = (Map<String, Object>) response.getEntity();
    assertTrue(contextData.containsKey("isHealthy"));
    assertTrue(contextData.containsKey("errors"));
  }

  @Test
  public void testGetReconErrors() {
    reconContext.updateErrors(ReconContext.ErrorCode.INVALID_NETWORK_TOPOLOGY);
    Response response = endpoint.getReconErrors();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    List<?> errors = (List<?>) response.getEntity();
    assertEquals(1, errors.size());
    assertTrue(errors.contains(ReconContext.ErrorCode.INVALID_NETWORK_TOPOLOGY));
  }

  @Test
  public void testIsReconHealthy() {
    Response response = endpoint.isReconHealthy();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(true, response.getEntity());

    // Set ReconContext to unhealthy
    reconContext.updateHealthStatus(new AtomicBoolean(false));
    response = endpoint.isReconHealthy();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(false, response.getEntity());
  }


  @Test
  public void testReconContextMultipleErrors() {
    reconContext.updateErrors(ReconContext.ErrorCode.INVALID_NETWORK_TOPOLOGY);
    reconContext.updateErrors(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED);

    Response response = endpoint.getReconErrors();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    List<?> errors = (List<?>) response.getEntity();
    assertEquals(2, errors.size());
    assertTrue(errors.contains(ReconContext.ErrorCode.INVALID_NETWORK_TOPOLOGY));
    assertTrue(errors.contains(ReconContext.ErrorCode.GET_OM_DB_SNAPSHOT_FAILED));
  }

  @Test
  public void testReconContextEmptyErrors() {
    Response response = endpoint.getReconErrors();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    List<?> errors = (List<?>) response.getEntity();
    assertTrue(errors.isEmpty());
  }

}
