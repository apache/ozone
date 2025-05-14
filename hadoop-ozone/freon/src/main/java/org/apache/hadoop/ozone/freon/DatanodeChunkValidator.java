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
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientCreator;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.kohsuke.MetaInfServices;
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
@MetaInfServices(FreonSubcommand.class)
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

  private XceiverClientSpi xceiverClient;

  private Timer timer;

  private ChecksumData checksumReference;

  private Checksum checksum;
  private ContainerProtos.ChecksumData checksumProtobuf;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConf = createOzoneConfiguration();
    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConf)) {
      throw new IllegalArgumentException(
              "Datanode chunk validator is not supported in secure environment"
      );
    }

    try (StorageContainerLocationProtocol scmClient =
                 createStorageContainerLocationClient(ozoneConf)) {
      Pipeline pipeline = findPipelineForTest(pipelineId, scmClient, LOG);

      try (XceiverClientFactory xceiverClientManager =
                   new XceiverClientCreator(ozoneConf)) {
        xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);

        checksumProtobuf = ContainerProtos.ChecksumData.newBuilder()
            .setBytesPerChecksum(4)
            .setType(ContainerProtos.ChecksumType.CRC32)
            .build();

        readReference();

        timer = getMetrics().timer("chunk-validate");

        runTests(this::validateChunk);

        xceiverClientManager.releaseClientForReadData(xceiverClient, true);
      }

    } finally {
      if (xceiverClient != null) {
        xceiverClient.close();
      }
    }
    return null;
  }

  /**
   * Read a reference chunk using same name than one from the
   * {@link org.apache.hadoop.ozone.freon.DatanodeChunkGenerator}.
   */
  private void readReference() throws IOException {
    ContainerCommandRequestProto request = createReadChunkRequest(0);
    ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request);

    checksum = new Checksum(ContainerProtos.ChecksumType.CRC32, chunkSize);
    checksumReference = computeChecksum(response);
  }

  private void validateChunk(long stepNo) throws Exception {
    ContainerCommandRequestProto request = createReadChunkRequest(stepNo);

    timer.time(() -> {
      try {
        ContainerCommandResponseProto response =
            xceiverClient.sendCommand(request);

        ChecksumData checksumOfChunk = computeChecksum(response);

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

  private ContainerCommandRequestProto createReadChunkRequest(long stepNo)
      throws IOException {
    ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(1L)
            .setLocalID(stepNo % 20)
            .build();

    ContainerProtos.ChunkInfo chunkInfo = ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName(getPrefix() + "_testdata_chunk_" + stepNo)
            .setChecksumData(checksumProtobuf)
            .setOffset((stepNo / 20) * chunkSize)
            .setLen(chunkSize)
            .build();

    ContainerProtos.ReadChunkRequestProto.Builder readChunkRequest =
        ContainerProtos.ReadChunkRequestProto
            .newBuilder()
            .setBlockID(blockId)
            .setChunkData(chunkInfo);

    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder builder =
            ContainerCommandRequestProto
                    .newBuilder()
                    .setCmdType(ContainerProtos.Type.ReadChunk)
                    .setContainerID(blockId.getContainerID())
                    .setDatanodeUuid(id)
                    .setReadChunk(readChunkRequest);

    return builder.build();
  }

  private ChecksumData computeChecksum(ContainerCommandResponseProto response)
      throws OzoneChecksumException {
    ContainerProtos.ReadChunkResponseProto readChunk = response.getReadChunk();
    if (readChunk.hasData()) {
      return checksum.computeChecksum(readChunk.getData().asReadOnlyByteBuffer());
    } else {
      return checksum.computeChecksum(
          readChunk.getDataBuffers().getBuffersList());
    }
  }
}
