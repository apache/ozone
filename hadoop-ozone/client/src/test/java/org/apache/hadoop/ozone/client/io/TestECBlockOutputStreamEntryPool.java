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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * {@link ECBlockOutputStreamEntryPool} tests: the storage tier of a block, as assigned by SCM, must end up as the
 * storage type of the EC block stream entry, so that it is sent to the datanodes.
 */
public class TestECBlockOutputStreamEntryPool {

  private ECBlockOutputStreamEntryPool pool;
  private Pipeline pipeline;

  @BeforeEach
  void setup() {
    ECReplicationConfig replicationConfig = new ECReplicationConfig("RS-3-2-1024k");
    List<DatanodeDetails> nodes = IntStream.range(0, replicationConfig.getRequiredNodes())
        .mapToObj(i -> DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build())
        .collect(Collectors.toList());
    pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(replicationConfig)
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(nodes)
        .build();

    ECKeyOutputStream.Builder builder = new ECKeyOutputStream.Builder().setReplicationConfig(replicationConfig);
    builder.setConfig(new OzoneClientConfig())
        .setHandler(new OpenKeySession(1, new OmKeyInfo.Builder()
            .setVolumeName("v").setBucketName("b").setKeyName("k").build(), 0))
        .setStreamBufferArgs(StreamBufferArgs.Builder.getNewBuilder()
            .setBufferSize(1024).setBufferFlushSize(1024).setBufferMaxSize(2048).build());
    pool = new ECBlockOutputStreamEntryPool(builder);
  }

  @ParameterizedTest
  @EnumSource(value = StorageTier.class, names = {"SSD", "DISK", "ARCHIVE"})
  void testEntryGetsStorageTypeOfBlockTier(StorageTier tier) {
    ECBlockOutputStreamEntry entry = pool.createStreamEntry(location(tier), false);

    assertThat(entry.getStorageType()).isEqualTo(tier.getUniformStorageType());
  }

  @Test
  void testEntryHasNoStorageTypeWhenBlockHasNoTier() {
    ECBlockOutputStreamEntry entry = pool.createStreamEntry(location(null), false);

    assertThat(entry.getStorageType()).isNull();
  }

  private OmKeyLocationInfo location(StorageTier tier) {
    return new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, 1))
        .setLength(1024)
        .setPipeline(pipeline)
        .setStorageTier(tier)
        .build();
  }
}
