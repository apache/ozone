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

package org.apache.hadoop.hdds.scm.pipeline;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Base class for testing {@link NCopyPipelineProvider} subclasses.
 */
public abstract class AbstractNCopyPipelineProviderTest {

  static DatanodeDetails createDatanodeDetails(boolean supportRatisStreaming) {
    Random random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);

    DatanodeDetails.Builder dn = DatanodeDetails.newBuilder()
        .setID(DatanodeID.randomID())
        .setHostName("localhost" + "-" + ipAddress)
        .setIpAddress(ipAddress)
        .setNetworkLocation(null)
        .setPersistedOpState(HddsProtos.NodeOperationalState.IN_SERVICE)
        .setPersistedOpStateExpiry(0);

    for (DatanodeDetails.Port.Name name : DatanodeDetails.Port.Name.values()) {
      if (!supportRatisStreaming && name == DatanodeDetails.Port.Name.RATIS_DATASTREAM) {
        continue;
      }
      dn.addPort(DatanodeDetails.newPort(name, 0));
    }
    return dn.build();
  }
}
