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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.api.Test;

/**
 * Tests the block location refresh function that {@link KeyInputStream} gives to each block stream.
 */
public class TestKeyInputStream {

  private static final BlockID BLOCK_ID = new BlockID(1, 1);

  @Test
  public void refreshReturnsNullWhenKeyLookupFails() throws IOException {
    Function<BlockID, BlockLocationInfo> refresh = getRefreshFunction(keyInfo -> {
      throw new IOException("lookup failed");
    });

    assertNull(refresh.apply(BLOCK_ID));
  }

  @Test
  public void refreshReturnsLocationFromRefreshedKey() throws IOException {
    OmKeyLocationInfo refreshed = newLocation(BLOCK_ID);
    Function<BlockID, BlockLocationInfo> refresh = getRefreshFunction(keyInfo -> newKeyInfo(refreshed));

    assertSame(refreshed, refresh.apply(BLOCK_ID));
  }

  @Test
  public void refreshReturnsNullWhenBlockIsNotInRefreshedKey() throws IOException {
    Function<BlockID, BlockLocationInfo> refresh =
        getRefreshFunction(keyInfo -> newKeyInfo(newLocation(new BlockID(2, 2))));

    assertNull(refresh.apply(BLOCK_ID));
  }

  private static Function<BlockID, BlockLocationInfo> getRefreshFunction(
      CheckedFunction<OmKeyInfo, OmKeyInfo, IOException> retryFunction) throws IOException {
    AtomicReference<Function<BlockID, BlockLocationInfo>> refresh = new AtomicReference<>();
    BlockInputStreamFactory factory = (repConfig, blockInfo, pipeline, token, xceiverFactory, refreshFunction,
        config) -> {
      refresh.set(refreshFunction);
      return mock(BlockExtendedInputStream.class);
    };
    KeyInputStream.getFromOmKeyInfo(newKeyInfo(newLocation(BLOCK_ID)), null, retryFunction, factory,
        new OzoneClientConfig()).close();
    return refresh.get();
  }

  private static OmKeyLocationInfo newLocation(BlockID blockID) {
    return new OmKeyLocationInfo.Builder().setBlockID(blockID).build();
  }

  private static OmKeyInfo newKeyInfo(OmKeyLocationInfo location) {
    return new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("key")
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, Collections.singletonList(location)))
        .build();
  }
}
