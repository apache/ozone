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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.conf.OMJobworkerConfiguration.getGrpcPortKey;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.PortAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for {@link JobworkerService}.
 */
@Timeout(300)
public class TestJobworkerService {
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private JobworkerService service;

  @BeforeEach
  public void setUp() {
    service = new JobworkerService(new String[] {});
  }

  @AfterEach
  public void tearDown() {
    if (service != null) {
      service.stop();
    }
  }

  /**
   * Configure for single OM setup.
   */
  private void configureSingleOM() {
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "127.0.0.1:9862");
  }

  @Test
  public void testConfigurationDefaults() {
    JobworkerConfiguration config = conf.getObject(JobworkerConfiguration.class);
    assertEquals(Duration.ofSeconds(10), config.getRpcTimeout());
    assertEquals(32 * 1024 * 1024, config.getGrpcMaximumInboundLength());
    assertEquals(Duration.ofSeconds(30), config.getHeartbeatInterval());
    assertEquals(10, config.getHeartbeatLogWarnInterval());
    assertEquals("", config.getStorageDirs());
    assertEquals(Duration.ofSeconds(60), config.getNodeReportInterval());
    assertEquals(4096, config.getMaxReportCount());
    assertEquals(8 * 1024 * 1024, config.getMaxReportSizeInBytes());
    assertEquals(100, config.getCommandQueueLimit());
    assertEquals(Duration.ofSeconds(30), config.getCommandStatusReportInterval());
    assertEquals("", config.getOmServiceIds());
  }

  @Test
  public void testConfigurationRoundTrip() {
    JobworkerConfiguration config = conf.getObject(JobworkerConfiguration.class);
    config.setRpcTimeout(Duration.ofSeconds(2));
    config.setGrpcMaximumInboundLength(2 * 1024 * 1024);
    config.setHeartbeatInterval(Duration.ofSeconds(5));
    config.setHeartbeatLogWarnInterval(7);
    config.setStorageVolumeDirs("/worker/data");
    config.setNodeReportInterval(Duration.ofSeconds(11));
    config.setMaxReportCount(13);
    config.setMaxReportSizeInBytes(4 * 1024 * 1024);
    config.setCommandQueueLimit(12);
    config.setCommandStatusReportInterval(Duration.ofSeconds(10));
    config.setOmServiceIds("service1,service2");
    conf.setFromObject(config);

    JobworkerConfiguration restored = conf.getObject(JobworkerConfiguration.class);
    assertEquals(config.getRpcTimeout(), restored.getRpcTimeout());
    assertEquals(config.getGrpcMaximumInboundLength(), restored.getGrpcMaximumInboundLength());
    assertEquals(config.getHeartbeatInterval(), restored.getHeartbeatInterval());
    assertEquals(config.getHeartbeatLogWarnInterval(), restored.getHeartbeatLogWarnInterval());
    assertEquals(config.getStorageDirs(), restored.getStorageDirs());
    assertEquals(config.getNodeReportInterval(), restored.getNodeReportInterval());
    assertEquals(config.getMaxReportCount(), restored.getMaxReportCount());
    assertEquals(config.getMaxReportSizeInBytes(), restored.getMaxReportSizeInBytes());
    assertEquals(config.getCommandQueueLimit(), restored.getCommandQueueLimit());
    assertEquals(config.getCommandStatusReportInterval(), restored.getCommandStatusReportInterval());
    assertEquals(config.getOmServiceIds(), restored.getOmServiceIds());
  }

  @Test
  public void testStartup() {
    configureSingleOM();
    service.start(conf);

    JobworkerDetails jobworkerDetails = service.getJobworkerDetails();
    assertNotNull(jobworkerDetails);
    assertNotNull(jobworkerDetails.getUuid());
    assertNotNull(jobworkerDetails.getIpAddress());
    assertNotNull(jobworkerDetails.getOperationalState());
    assertNotNull(jobworkerDetails.getJobworkerVersion());
    assertNotNull(jobworkerDetails.getVersion());
    assertNotNull(jobworkerDetails.getBuildDate());
    assertNotNull(jobworkerDetails.getRevision());
    assertThat(jobworkerDetails.getSetupTime()).isGreaterThan(0);

    service.stop();
    service.join();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  public void testConnectionManagerWithHAOMs(int omGroupCount) throws InterruptedException, TimeoutException {
    int omCountPerGroup = 3;
    Set<String> exceptedServiceIds = configureOMs(omGroupCount, omCountPerGroup);
    service.start(conf);

    JobworkerConnectionManager connManager =
        service.getJobworkerStateMachine().getConnectionManager();

    GenericTestUtils.waitFor(() ->
            service.getJobworkerStateMachine().getContext().getState() == JobworkerStates.RUNNING,
        200, 10000);
    assertEquals(omCountPerGroup * omGroupCount, connManager.getAllEndpoints().size());
    service.stop();
    service.join();
  }

  /**
   * Configure for HA OM setup.
   */
  private Set<String> configureOMs(int omGroupCount, int omCountPerGroup) {
    Set<String> serviceIds = new HashSet<>();
    for (int i = 0; i < omGroupCount; i++) {
      String serviceId = "omservice" + i;
      serviceIds.add(serviceId);
      ArrayList<String> omIds = new ArrayList<>();
      for (int j = 0; j < omCountPerGroup; j++) {
        String omId = "om" + j;
        omIds.add(omId);
        // This OM address will not be really used, so we can configure any port
        conf.set(getGrpcPortKey() + "." + serviceId + "." + omId, String.valueOf(PortAllocator.getFreePort()));
        conf.set(OZONE_OM_ADDRESS_KEY + "." + serviceId + "." + omId, "localhost");
      }
      conf.set(OMConfigKeys.OZONE_OM_NODES_KEY + "." + serviceId, String.join(", ", omIds));
    }
    conf.set(JobworkerConfiguration.OZONE_JOBWORKER_OM_SERVICE_IDS_KEY, String.join(",", serviceIds));
    return serviceIds;
  }
}
