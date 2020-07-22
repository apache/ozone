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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for ReconControllerModule.
 */
public class TestReconControllerModule {

  @Test
  public void testGetOzoneManagerServiceId() throws IOException {
    ReconControllerModule reconControllerModule = new ReconControllerModule();

    OzoneConfiguration configuration = new OzoneConfiguration();
    try {
      reconControllerModule.getOzoneManagerServiceId(configuration);
      Assert.fail();
    } catch (IOException ioEx) {
      assertTrue(ioEx.getMessage()
          .contains("No OzoneManager ServiceID configured to work"));
    }

    configuration.set(OZONE_OM_INTERNAL_SERVICE_ID, "om1");
    String id = reconControllerModule.getOzoneManagerServiceId(configuration);
    assertEquals("om1", id);

    configuration.set(OZONE_OM_SERVICE_IDS_KEY, "om2");
    id = reconControllerModule.getOzoneManagerServiceId(configuration);
    assertEquals("om1", id);

    configuration = new OzoneConfiguration();
    configuration.set(OZONE_OM_SERVICE_IDS_KEY, "om2");
    id = reconControllerModule.getOzoneManagerServiceId(configuration);
    assertEquals("om2", id);
  }
}
