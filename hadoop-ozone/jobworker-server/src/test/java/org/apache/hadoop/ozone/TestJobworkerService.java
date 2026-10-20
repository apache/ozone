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

import java.io.IOException;
import java.nio.file.Path;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for {@link JobworkerService}.
 */
@Timeout(300)
public class TestJobworkerService {
  @TempDir
  private Path directory;

  private final OzoneConfiguration conf = new OzoneConfiguration();
  private JobworkerService service;

  @BeforeEach
  public void setUp() throws IOException {
    conf.set("ozone.jobworker.metadata.dir", directory.resolve("worker-state").toString());
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
