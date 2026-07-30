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

package org.apache.hadoop.ozone.om;

import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.QueryUpgradeStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to help test OM upgrade scenarios.
 */
public final class OMUpgradeTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(OMUpgradeTestUtils.class);

  private OMUpgradeTestUtils() {
    // Utility class.
  }

  public static void waitForFinalization(OzoneManagerProtocol omClient)
      throws TimeoutException, InterruptedException {
    waitFor(() -> {
      try {
        QueryUpgradeStatusResponse status = omClient.queryUpgradeStatus();
        HddsProtos.UpgradeStatus hdds = status.getHddsStatus();
        LOG.info("Finalization status: omFinalized={}, scmFinalized={}, datanodes={}/{}",
            status.getOmFinalized(), hdds.getScmFinalized(),
            hdds.getNumDatanodesFinalized(), hdds.getNumDatanodesTotal());
        return status.getOmFinalized()
            && hdds.getScmFinalized()
            && hdds.getNumDatanodesFinalized() == hdds.getNumDatanodesTotal();
      } catch (IOException e) {
        fail(e.getMessage());
      }
      return false;
    }, 2000, 20000);
  }
}
