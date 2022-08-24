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
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.common.MonotonicClock;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import java.time.ZoneOffset;

/**
 * {@link BlockOutputStreamEntryPool} is responsible to manage OM communication
 * regarding writing a block to Ozone in a non-EC write case.
 * The basic operations are fine for us but we need a specific
 * {@link ECBlockOutputStreamEntry} implementation to handle writing EC block
 * groups, this class implements the logic that handles the specific EC entries'
 * instantiation and retrieval from the pool.
 *
 * @see ECKeyOutputStream
 * @see BlockOutputStreamEntryPool
 * @see ECBlockOutputStreamEntry
 */
public class ECBlockOutputStreamEntryPool extends BlockOutputStreamEntryPool {

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
      long openID,
      ContainerClientMetrics clientMetrics) {
    super(config, omClient, requestId, replicationConfig, uploadID, partNumber,
        isMultipart, info, unsafeByteBufferConversion, xceiverClientFactory,
        openID, clientMetrics);
    assert replicationConfig instanceof ECReplicationConfig;
  }

  @Override
  ExcludeList createExcludeList() {
    return new ExcludeList(getConfig().getExcludeNodesExpiryTime(),
        new MonotonicClock(ZoneOffset.UTC));
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
            .setClientMetrics(getClientMetrics())
            .build();
  }

  @Override
  public ECBlockOutputStreamEntry getCurrentStreamEntry() {
    return (ECBlockOutputStreamEntry) super.getCurrentStreamEntry();
  }
}
