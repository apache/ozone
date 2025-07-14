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

package org.apache.hadoop.ozone.recon.api.filters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;
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
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

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

    assertThat(allEndpoints).isNotEmpty();

    // If an endpoint is added, it must either require admin privileges by being
    // marked with the `@AdminOnly` annotation, or be added to this set to exclude it.
    // - Any endpoint that displays information related to the filesystem namespace
    //   (including aggregate counts), user information, or allows modification to the
    //   cluster's state should be marked as `@AdminOnly`.
    // - Read-only endpoints that only return information about node status or
    //   cluster state do not require the `@AdminOnly` annotation and can be excluded
    //   from admin requirements by adding them to this set.
    Set<Class<?>> nonAdminEndpoints = new HashSet<>();
    nonAdminEndpoints.add(UtilizationEndpoint.class);
    nonAdminEndpoints.add(ClusterStateEndpoint.class);
    nonAdminEndpoints.add(MetricsProxyEndpoint.class);
    nonAdminEndpoints.add(NodeEndpoint.class);
    nonAdminEndpoints.add(PipelineEndpoint.class);
    nonAdminEndpoints.add(TaskStatusService.class);

    assertThat(allEndpoints).containsAll(nonAdminEndpoints);

    Set<Class<?>> adminEndpoints = Sets.difference(allEndpoints,
        nonAdminEndpoints);

    for (Class<?> endpoint: nonAdminEndpoints) {
      assertFalse(endpoint.isAnnotationPresent(AdminOnly.class),
          String.format("Endpoint class %s has been declared as non admin " +
              "in this test, but is marked as @AdminOnly.", endpoint));
    }

    for (Class<?> endpoint: adminEndpoints) {
      assertTrue(endpoint.isAnnotationPresent(AdminOnly.class),
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
    Principal mockPrincipal = mock(Principal.class);
    when(mockPrincipal.getName()).thenReturn(principalToUse);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getUserPrincipal()).thenReturn(mockPrincipal);

    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    FilterChain mockFilterChain = mock(FilterChain.class);

    new ReconAdminFilter(conf).doFilter(mockRequest, mockResponse,
        mockFilterChain);

    if (shouldPass) {
      verify(mockFilterChain).doFilter(mockRequest, mockResponse);
    } else {
      verify(mockResponse).setStatus(HttpServletResponse.SC_FORBIDDEN);
    }
  }
}
