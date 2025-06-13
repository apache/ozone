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

package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.scm.XceiverClientCreator;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Checksum;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Datanode test for block creation.
 */
@Command(name = "dbp",
    aliases = "datanode-block-putter",
    description = "Issues putBlock commands to a Ratis pipeline.  The " +
        "blocks are associated with a list of fake chunks, which do not " +
        "really exist.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
@SuppressWarnings("java:S2245") // no need for secure random
public class DatanodeBlockPutter extends BaseFreonGenerator implements
    Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeBlockPutter.class);

  @Option(names = {"-c", "--chunks-per-block"},
      description = "Number of chunks to include in each putBlock",
      defaultValue = "4")
  private int chunksPerBlock;

  @Option(names = {"-s", "--size"},
      description = "Size of the fake chunks (in bytes)",
      defaultValue = "1024")
  private int chunkSize;

  @Option(names = {"-l", "--pipeline"},
      description = "Pipeline to use. By default the first RATIS/THREE "
          + "pipeline will be used.",
      defaultValue = "")
  private String pipelineId;

  private XceiverClientSpi client;

  private Timer timer;

  private ChecksumData checksumProtobuf;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConf = createOzoneConfiguration();
    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConf)) {
      throw new IllegalArgumentException(
          "datanode-block-putter is not supported in secure environment");
    }

    try (StorageContainerLocationProtocol scmLocationClient =
        createStorageContainerLocationClient(ozoneConf)) {
      Pipeline pipeline =
          findPipelineForTest(pipelineId, scmLocationClient, LOG);

      try (XceiverClientFactory xceiverClientManager =
               new XceiverClientCreator(ozoneConf)) {
        client = xceiverClientManager.acquireClient(pipeline);

        timer = getMetrics().timer("put-block");

        byte[] data = RandomStringUtils.secure().nextAscii(chunkSize)
            .getBytes(StandardCharsets.UTF_8);
        Checksum checksum = new Checksum(ChecksumType.CRC32, 1024 * 1024);
        checksumProtobuf = checksum.computeChecksum(data).getProtoBufMessage();

        runTests(this::putBlock);
      }
    } finally {
      if (client != null) {
        client.close();
      }
    }
    return null;
  }

  private void putBlock(long stepNo) throws Exception {
    ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(1L)
            .setLocalID(stepNo)
            .setBlockCommitSequenceId(stepNo)
            .build();

    BlockData.Builder blockData = BlockData.newBuilder()
        .setBlockID(blockId);
    for (long l = 0; l < chunksPerBlock; l++) {
      ChunkInfo.Builder chunkInfo = ChunkInfo.newBuilder()
          .setChunkName(getPrefix() + "_chunk_" + stepNo)
          .setOffset(l * chunkSize)
          .setLen(chunkSize)
          .setChecksumData(checksumProtobuf);
      blockData.addChunks(chunkInfo);
    }

    PutBlockRequestProto.Builder putBlockRequest = PutBlockRequestProto
        .newBuilder()
        .setBlockData(blockData);

    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto
            .newBuilder()
            .setCmdType(Type.PutBlock)
            .setContainerID(blockId.getContainerID())
            .setDatanodeUuid(id)
            .setPutBlock(putBlockRequest);

    ContainerCommandRequestProto request = builder.build();
    timer.time(() -> {
      client.sendCommand(request);
      return null;
    });
  }

}
