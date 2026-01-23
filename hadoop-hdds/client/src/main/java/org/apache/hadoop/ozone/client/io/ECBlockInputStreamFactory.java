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

package org.apache.hadoop.ozone.client.io;

import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;

/**
 * Interface used by factories which create ECBlockInput streams for
 * reconstruction or non-reconstruction reads.
 */
public interface ECBlockInputStreamFactory {

  /**
   * Create a new EC InputStream based on the missingLocations boolean. If it is
   * set to false, it indicates all locations are available and an
   * ECBlockInputStream will be created. Otherwise an
   * ECBlockReconstructedInputStream will be created.
   * @param missingLocations Indicates if all the data locations are available
   *                         or not, controlling the type of stream created
   * @param failedLocations List of DatanodeDetails indicating locations we
   *                        know are bad and should not be used.
   * @param repConfig The replication Config
   * @param blockInfo The blockInfo representing the block.
   * @param xceiverFactory Factory to create the xceiver in the client
   * @param refreshFunction Function to refresh the block location if needed
   * @return BlockExtendedInputStream of the correct type.
   */
  BlockExtendedInputStream create(boolean missingLocations,
      List<DatanodeDetails> failedLocations, ReplicationConfig repConfig,
      BlockLocationInfo blockInfo,
      XceiverClientFactory xceiverFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config);
}
