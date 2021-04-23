/*
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
package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data validator of chunks to use pure datanode XCeiver interface.
 */
@Command(name = "dcv",
    aliases = "datanode-chunk-validator",
    description = "Validate generated Chunks are the same ",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class DatanodeChunkValidator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeChunkValidator.class);

  @Option(names = {"-l", "--pipeline"},
          description = "Pipeline to use. By default the first RATIS/THREE "
                  + "pipeline will be used.",
          defaultValue = "")
  private String pipelineId;

  @Option(names = {"-s", "--size"},
          description = "Size of the generated chunks (in bytes)",
          defaultValue = "1024")
  private int chunkSize;

  private XceiverClientSpi xceiverClientSpi;

  private Timer timer;

  private ChecksumData checksumReference;

  private Checksum checksum;


  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConf = createOzoneConfiguration();
    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConf)) {
      throw new IllegalArgumentException(
              "Datanode chunk validator is not supported in secure environment"
      );
    }

    try (StorageContainerLocationProtocol scmLocationClient =
                 createStorageContainerLocationClient(ozoneConf)) {
      List<Pipeline> pipelines = scmLocationClient.listPipelines();
      Pipeline pipeline;
      if (pipelineId != null && pipelineId.length() > 0) {
        pipeline = pipelines.stream()
              .filter(p -> p.getId().toString().equals(pipelineId))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException(
                      "Pipeline ID is defined, but there is no such pipeline: "
                              + pipelineId));

      } else {
        pipeline = pipelines.stream()
            .filter(
                p -> ReplicationConfig.getLegacyFactor(p.getReplicationConfig())
                    == HddsProtos.ReplicationFactor.THREE)
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException(
                      "Pipeline ID is NOT defined, and no pipeline " +
                              "has been found with factor=THREE"));
        LOG.info("Using pipeline {}", pipeline.getId());
      }

      try (XceiverClientManager xceiverClientManager =
                   new XceiverClientManager(ozoneConf)) {
        xceiverClientSpi = xceiverClientManager.acquireClient(pipeline);

        readReference();

        timer = getMetrics().timer("chunk-validate");

        runTests(this::validateChunk);
      }

    } finally {
      if (xceiverClientSpi != null) {
        xceiverClientSpi.close();
      }
    }
    return null;
  }

  /**
   * Read a reference chunk using same name than one from the
   * {@link org.apache.hadoop.ozone.freon.DatanodeChunkGenerator}.
   * @throws IOException
   */
  private void readReference() throws IOException {
    ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(1L)
            .setLocalID(0 % 20)
            .setBlockCommitSequenceId(0)
            .build();

    // As a reference, the first one generated (at step 0) is taken
    ContainerProtos.ChunkInfo chunkInfo = ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName(getPrefix() + "_testdata_chunk_" + 0)
            .setOffset((0 / 20) * chunkSize)
            .setLen(chunkSize)
            .setChecksumData(
                    ContainerProtos.ChecksumData.newBuilder()
                            .setBytesPerChecksum(4)
                            .setType(ContainerProtos.ChecksumType.CRC32)
                            .build())
            .build();

    ContainerProtos.ReadChunkRequestProto.Builder readChunkRequest =
        ContainerProtos.ReadChunkRequestProto
            .newBuilder()
            .setBlockID(blockId)
            .setChunkData(chunkInfo);

    String id = xceiverClientSpi.getPipeline().getFirstNode().getUuidString();

    ContainerProtos.ContainerCommandRequestProto.Builder builder =
            ContainerProtos.ContainerCommandRequestProto
                    .newBuilder()
                    .setCmdType(ContainerProtos.Type.ReadChunk)
                    .setContainerID(blockId.getContainerID())
                    .setDatanodeUuid(id)
                    .setReadChunk(readChunkRequest);

    ContainerProtos.ContainerCommandRequestProto request = builder.build();
    ContainerProtos.ContainerCommandResponseProto response =
        xceiverClientSpi.sendCommand(request);

    checksum = new Checksum(ContainerProtos.ChecksumType.CRC32, chunkSize);
    if (response.getReadChunk().hasData()) {
      checksumReference = checksum.computeChecksum(
          response.getReadChunk().getData().toByteArray());
    } else {
      checksumReference = checksum.computeChecksum(
          response.getReadChunk().getDataBuffers().getBuffersList());
    }

  }


  private void validateChunk(long stepNo) throws Exception {
    ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(1L)
            .setLocalID(stepNo % 20)
            .setBlockCommitSequenceId(stepNo)
            .build();

    ContainerProtos.ChunkInfo chunkInfo = ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName(getPrefix() + "_testdata_chunk_" + stepNo)
            .setChecksumData(
                    ContainerProtos.ChecksumData.newBuilder()
                            .setBytesPerChecksum(4)
                            .setType(ContainerProtos.ChecksumType.CRC32)
                            .build())
            .setOffset((stepNo / 20) * chunkSize)
            .setLen(chunkSize)
            .build();

    ContainerProtos.ReadChunkRequestProto.Builder readChunkRequest =
        ContainerProtos.ReadChunkRequestProto
            .newBuilder()
            .setBlockID(blockId)
            .setChunkData(chunkInfo);

    String id = xceiverClientSpi.getPipeline().getFirstNode().getUuidString();

    ContainerProtos.ContainerCommandRequestProto.Builder builder =
            ContainerProtos.ContainerCommandRequestProto
                    .newBuilder()
                    .setCmdType(ContainerProtos.Type.ReadChunk)
                    .setContainerID(blockId.getContainerID())
                    .setDatanodeUuid(id)
                    .setReadChunk(readChunkRequest);

    ContainerProtos.ContainerCommandRequestProto request = builder.build();

    timer.time(() -> {
      try {
        ContainerProtos.ContainerCommandResponseProto response =
            xceiverClientSpi.sendCommand(request);

        ChecksumData checksumOfChunk;
        if (response.getReadChunk().hasData()) {
          checksumOfChunk = checksum.computeChecksum(
              response.getReadChunk().getData().toByteArray());
        } else {
          checksumOfChunk = checksum.computeChecksum(
              response.getReadChunk().getDataBuffers().getBuffersList());
        }

        if (!checksumReference.equals(checksumOfChunk)) {
          throw new IllegalStateException(
              "Reference (=first) message checksum doesn't match " +
                  "with checksum of chunk "
                        + response.getReadChunk()
                  .getChunkData().getChunkName());
        }
      } catch (IOException e) {
        LOG.warn("Could not read chunk due to IOException: ", e);
      }
    });

  }


}
