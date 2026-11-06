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

package org.apache.hadoop.ozone.recon.common;

import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ReconReplicaHistoryDBProvider;

/**
 * This is a utility class for common code for test cases.
 */
public final class ReconTestUtils {

  public static SCMNodeDetails getReconNodeDetails() {
    SCMNodeDetails.Builder builder = new SCMNodeDetails.Builder();
    builder.setSCMNodeId("Recon");
    builder.setDatanodeProtocolServerAddress(
        InetSocketAddress.createUnresolved("127.0.0.1", 9888));
    return builder.build();
  }

  public static void closeReconResources(OzoneStorageContainerManager reconScm, ReconDBProvider reconDBProvider,
      ReconReplicaHistoryDBProvider replicaHistoryDBProvider, ReconOMMetadataManager reconOMMetadataManager,
      OMMetadataManager omMetadataManager) throws Exception {
    // Stop SCM before closing the providers; stop() flushes replica history to the replica history DB.
    if (reconScm != null) {
      reconScm.stop();
    }
    if (reconDBProvider != null) {
      reconDBProvider.close();
    }
    if (replicaHistoryDBProvider != null) {
      replicaHistoryDBProvider.close();
    }
    if (reconOMMetadataManager != null) {
      reconOMMetadataManager.stop();
    }
    if (omMetadataManager != null) {
      omMetadataManager.stop();
    }
  }

  private ReconTestUtils() {
    // no instances
  }
}
