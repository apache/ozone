/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc.read;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientShortCircuit;
import org.apache.hadoop.hdds.scm.storage.ShortCircuitChunkInputStream;
import org.apache.ozone.test.GenericTestUtils;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT;

import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;

import java.io.File;

/**
 * Tests {@link ShortCircuitChunkInputStream}.
 */
public class TestShortCircuitChunkInputStream extends TestChunkInputStream {

  @TempDir
  private File dir;

  @Override
  int getDatanodeCount() {
    return 1;
  }

  @Override
  void setCustomizedProperties(OzoneConfiguration configuration) {
    configuration.setBoolean(OZONE_READ_SHORT_CIRCUIT, true);
    configuration.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-dn-socket").getAbsolutePath());
    GenericTestUtils.setLogLevel(XceiverClientShortCircuit.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(XceiverClientGrpc.LOG, Level.DEBUG);
  }

  @Override
  ReplicationConfig getRepConfig() {
    return RatisReplicationConfig.getInstance(ONE);
  }
}
