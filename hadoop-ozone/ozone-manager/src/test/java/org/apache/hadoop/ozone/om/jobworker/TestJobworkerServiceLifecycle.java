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

package org.apache.hadoop.ozone.om.jobworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.JobworkerService;
import org.apache.hadoop.ozone.conf.OMJobworkerConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression coverage for optional framework startup and real worker registration.
 */
class TestJobworkerServiceLifecycle {
  @TempDir
  private Path directory;

  @Test
  void testDisabledByDefault() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, directory.toString());
    assertFalse(conf.getObject(OMJobworkerConfiguration.class).isEnabled());
    OmTestManagers managers = new OmTestManagers(conf);
    try {
      OzoneManager om = managers.getOzoneManager();
      assertNull(om.getJobworkerNodemanager());
      assertNull(om.getOMJobworkerCommandManager());
      assertNull(om.getJobworkerEventQueue());
      assertEquals(-1, om.getJobworkerGrpcPort());
      assertNotNull(managers.getWriteClient().getServiceInfo());
    } finally {
      managers.stop();
    }
  }

  @Test
  void testWorkerReregistersAfterOMRestart() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, directory.toString());
    conf.setBoolean("ozone.om.jobworker.service.enabled", true);
    conf.setInt(OMJobworkerConfiguration.getGrpcPortKey(), 0);
    OmTestManagers managers = new OmTestManagers(conf);
    JobworkerService worker = new JobworkerService(new String[0]);
    try {
      OzoneManager om = managers.getOzoneManager();
      int port = om.getJobworkerGrpcPort();
      // Keep the endpoint stable across restart so the existing worker can reconnect.
      om.getConfiguration().setInt(OMJobworkerConfiguration.getGrpcPortKey(), port);
      OzoneConfiguration workerConf = new OzoneConfiguration(conf);
      workerConf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "localhost");
      workerConf.setInt(OMJobworkerConfiguration.getGrpcPortKey(), port);
      workerConf.set("ozone.jobworker.storage.volume.dirs", directory.resolve("worker").toString());
      workerConf.set("ozone.jobworker.heartbeat.interval", "100ms");
      workerConf.set("ozone.jobworker.node.report.interval", "100ms");
      workerConf.set("ozone.jobworker.command.status.report.interval", "100ms");
      worker.start(workerConf);
      UUID workerId = worker.getJobworkerDetails().getUuid();
      GenericTestUtils.waitFor(
          () -> om.getJobworkerNodemanager().isJobworkerNodeRegistered(workerId), 100, 20_000);
      om.stop();
      om.restart();
      GenericTestUtils.waitFor(
          () -> om.getJobworkerNodemanager().isJobworkerNodeRegistered(workerId), 100, 20_000);
      assertEquals(workerId, worker.getJobworkerDetails().getUuid());
      assertNotNull(om.getOMJobworkerCommandManager());
    } finally {
      worker.stop();
      managers.stop();
    }
  }
}
