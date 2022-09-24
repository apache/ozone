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
package org.apache.hadoop.ozone.recon.api.filters;

import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.recon.api.AdminOnly;
import org.apache.hadoop.ozone.recon.api.ClusterStateEndpoint;
import org.apache.hadoop.ozone.recon.api.MetricsProxyEndpoint;
import org.apache.hadoop.ozone.recon.api.NodeEndpoint;
import org.apache.hadoop.ozone.recon.api.PipelineEndpoint;
import org.apache.hadoop.ozone.recon.api.TaskStatusService;
import org.apache.hadoop.ozone.recon.api.UtilizationEndpoint;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests the admin filter on recon endpoints which should only be accessible
 * to admins.
 */
public class TestAdminFilter {
  /**
   * Tests that only designated endpoints are not marked with the @AdminOnly
   * annotation, meaning they are accessible to all users.
   */
  @Test
  public void testAdminOnlyEndpoints() {
    // Get all classes with @Path annotation anywhere in recon.
    Reflections reflections = new Reflections(
        "org.apache.hadoop.ozone.recon",
        new TypeAnnotationsScanner(),
        new SubTypesScanner());
    Set<Class<?>> allEndpoints =
        reflections.getTypesAnnotatedWith(Path.class);

    Assertions.assertFalse(allEndpoints.isEmpty());

    // If an endpoint is added, it must be explicitly added to this set or be
    // marked with @AdminOnly for this test to pass.
    Set<Class<?>> nonAdminEndpoints = new HashSet<>();
    nonAdminEndpoints.add(UtilizationEndpoint.class);
    nonAdminEndpoints.add(ClusterStateEndpoint.class);
    nonAdminEndpoints.add(MetricsProxyEndpoint.class);
    nonAdminEndpoints.add(NodeEndpoint.class);
    nonAdminEndpoints.add(PipelineEndpoint.class);
    nonAdminEndpoints.add(TaskStatusService.class);

    Assertions.assertTrue(allEndpoints.containsAll(nonAdminEndpoints));

    Set<Class<?>> adminEndpoints = Sets.difference(allEndpoints,
        nonAdminEndpoints);

    for (Class<?> endpoint: nonAdminEndpoints) {
      Assertions.assertFalse(endpoint.isAnnotationPresent(AdminOnly.class),
          String.format("Endpoint class %s has been declared as non admin " +
              "in this test, but is marked as @AdminOnly.", endpoint));
    }

    for (Class<?> endpoint: adminEndpoints) {
      Assertions.assertTrue(endpoint.isAnnotationPresent(AdminOnly.class),
          String.format("Endpoint class %s must be marked as @AdminOnly " +
              "or explicitly declared as non admin in this test.", endpoint));
    }
  }

  @Test
  public void testAdminFilterOzoneAdminsOnly() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS, "ozone");
    testAdminFilterWithPrincipal(conf, "ozone", true);
    testAdminFilterWithPrincipal(conf, "reject", false);

    conf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD);
    testAdminFilterWithPrincipal(conf, "other", true);

    UserGroupInformation.createUserForTesting("user1",
        new String[]{"admingroup"});
    try {
      conf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS, "ozone");
      conf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS,
          "admingroup");
      testAdminFilterWithPrincipal(conf, "user1", true);
    } finally {
      UserGroupInformation.reset();
    }
  }

  @Test
  public void testAdminFilterReconAdminsOnly() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStrings(ReconConfigKeys.OZONE_RECON_ADMINISTRATORS, "recon");
    testAdminFilterWithPrincipal(conf, "recon", true);
    testAdminFilterWithPrincipal(conf, "reject", false);

    conf.setStrings(ReconConfigKeys.OZONE_RECON_ADMINISTRATORS,
        OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD);
    testAdminFilterWithPrincipal(conf, "other", true);

    UserGroupInformation.createUserForTesting("user1",
        new String[]{"reconadmingroup"});
    try {
      conf.setStrings(ReconConfigKeys.OZONE_RECON_ADMINISTRATORS, "recon");
      conf.setStrings(ReconConfigKeys.OZONE_RECON_ADMINISTRATORS_GROUPS,
          "reconadmingroup");
      testAdminFilterWithPrincipal(conf, "user1", true);
    } finally {
      UserGroupInformation.reset();
    }
  }

  @Test
  public void testAdminFilterOzoneAndReconAdmins() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS, "ozone");
    conf.setStrings(ReconConfigKeys.OZONE_RECON_ADMINISTRATORS, "recon");
    testAdminFilterWithPrincipal(conf, "ozone", true);
    testAdminFilterWithPrincipal(conf, "recon", true);
    testAdminFilterWithPrincipal(conf, "reject", false);

    conf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD);
    conf.setStrings(ReconConfigKeys.OZONE_RECON_ADMINISTRATORS,
        OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD);
    testAdminFilterWithPrincipal(conf, "other", true);
  }

  @Test
  public void testAdminFilterNoAdmins() throws Exception {
    testAdminFilterWithPrincipal(new OzoneConfiguration(), "reject", false);
  }

  private void testAdminFilterWithPrincipal(OzoneConfiguration conf,
      String principalToUse, boolean shouldPass) throws Exception {
    Principal mockPrincipal = Mockito.mock(Principal.class);
    Mockito.when(mockPrincipal.getName()).thenReturn(principalToUse);
    HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockRequest.getUserPrincipal()).thenReturn(mockPrincipal);

    HttpServletResponse mockResponse = Mockito.mock(HttpServletResponse.class);
    FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

    new ReconAdminFilter(conf).doFilter(mockRequest, mockResponse,
        mockFilterChain);

    if (shouldPass) {
      Mockito.verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    } else {
      Mockito.verify(mockResponse).setStatus(HttpServletResponse.SC_FORBIDDEN);
    }
  }
}
