/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import java.util.ArrayList;
import java.util.List;

/**
 * This class manages the stream entries list and handles block allocation
 * from OzoneManager for EC writes.
 */
public class ECBlockOutputStreamEntryPool extends BlockOutputStreamEntryPool {
  private final List<BlockOutputStreamEntry> finishedStreamEntries;
  private final ECReplicationConfig ecReplicationConfig;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public ECBlockOutputStreamEntryPool(OzoneClientConfig config,
      OzoneManagerProtocol omClient,
      String requestId,
      ReplicationConfig replicationConfig,
      String uploadID,
      int partNumber,
      boolean isMultipart,
      OmKeyInfo info,
      boolean unsafeByteBufferConversion,
      XceiverClientFactory xceiverClientFactory,
      long openID) {
    super(config, omClient, requestId, replicationConfig, uploadID, partNumber,
        isMultipart, info, unsafeByteBufferConversion, xceiverClientFactory,
        openID);
    this.finishedStreamEntries = new ArrayList<>();
    assert replicationConfig instanceof ECReplicationConfig;
    this.ecReplicationConfig = (ECReplicationConfig) replicationConfig;
  }

  @Override
  BlockOutputStreamEntry createStreamEntry(OmKeyLocationInfo subKeyInfo) {
    return
        new ECBlockOutputStreamEntry.Builder()
            .setBlockID(subKeyInfo.getBlockID())
            .setKey(getKeyName())
            .setXceiverClientManager(getXceiverClientFactory())
            .setPipeline(subKeyInfo.getPipeline())
            .setConfig(getConfig())
            .setLength(subKeyInfo.getLength())
            .setBufferPool(getBufferPool())
            .setToken(subKeyInfo.getToken())
            .build();
  }

  @Override
  public ECBlockOutputStreamEntry getCurrentStreamEntry() {
    return (ECBlockOutputStreamEntry) super.getCurrentStreamEntry();
  }
}
