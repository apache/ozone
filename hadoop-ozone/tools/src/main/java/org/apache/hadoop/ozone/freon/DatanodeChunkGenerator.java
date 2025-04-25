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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.XceiverClientCreator;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator to use pure datanode XCeiver interface.
 */
@Command(name = "dcg",
    aliases = "datanode-chunk-generator",
    description = "Create as many chunks as possible with pure XCeiverClient.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
@SuppressWarnings("java:S2245") // no need for secure random
public class DatanodeChunkGenerator extends BaseFreonGenerator implements
    Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeChunkGenerator.class);

  @Option(names = {"-a", "--async"},
      description = "Use async operation.",
      defaultValue = "false")
  private boolean async;

  @Option(names = {"-s", "--size"},
      description = "Size of the generated chunks (in bytes)",
      defaultValue = "1024")
  private int chunkSize;

  @Option(names = {"-l", "--pipeline"},
      description = "Pipeline to use. By default the first RATIS/THREE "
          + "pipeline will be used.",
      defaultValue = "")
  private String pipelineIds;

  @Option(names = {"-d", "--datanodes"},
      description = "Datanodes to use." +
          " Test will write to all the existing pipelines " +
          "which this datanode is member of.",
      defaultValue = "")
  private String datanodes;

  private List<XceiverClientSpi> xceiverClients;

  private Timer timer;

  private ByteString dataToWrite;
  private ChecksumData checksumProtobuf;

  @Override
  public Void call() throws Exception {

    OzoneConfiguration ozoneConf = createOzoneConfiguration();
    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConf)) {
      throw new IllegalArgumentException(
          "Datanode chunk generator is not supported in secure environment");
    }

    List<String> pipelinesFromCmd = Arrays.asList(pipelineIds.split(","));

    List<String> datanodeHosts = Arrays.asList(this.datanodes.split(","));

    Set<Pipeline> pipelines;

    try (StorageContainerLocationProtocol scmLocationClient =
               createStorageContainerLocationClient(ozoneConf);
         XceiverClientFactory xceiverClientManager =
             new XceiverClientCreator(ozoneConf)) {
      List<Pipeline> pipelinesFromSCM = scmLocationClient.listPipelines();
      Pipeline firstPipeline;
      init();
      if (!arePipelinesOrDatanodesProvided()) {
        //default behaviour if no arguments provided
        firstPipeline = pipelinesFromSCM.stream()
              .filter(p -> p.getReplicationConfig().getRequiredNodes() == 3)
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException(
                  "Pipeline ID is NOT defined, and no pipeline " +
                      "has been found with factor=THREE"));
        XceiverClientSpi xceiverClientSpi = xceiverClientManager
            .acquireClient(firstPipeline);
        xceiverClients = new ArrayList<>();
        xceiverClients.add(xceiverClientSpi);
      } else {
        xceiverClients = new ArrayList<>();
        pipelines = new HashSet<>();
        for (String pipelineId:pipelinesFromCmd) {
          List<Pipeline> selectedPipelines =  pipelinesFromSCM.stream()
              .filter((p -> p.getId().toString()
                  .equals("PipelineID=" + pipelineId)
                  || pipelineContainsDatanode(p, datanodeHosts)))
               .collect(Collectors.toList());
          pipelines.addAll(selectedPipelines);
        }
        for (Pipeline p:pipelines) {
          LOG.info("Writing to pipeline: " + p.getId());
          xceiverClients.add(xceiverClientManager.acquireClient(p));
        }
        if (pipelines.isEmpty()) {
          throw new IllegalArgumentException(
              "Couldn't find the any/the selected pipeline");
        }
      }
      runTest();
    } finally {
      for (XceiverClientSpi xceiverClientSpi : xceiverClients) {
        if (xceiverClientSpi != null) {
          xceiverClientSpi.close();
        }
      }
    }
    return null;
  }

  private boolean pipelineContainsDatanode(Pipeline p,
      List<String> datanodeHosts) {
    for (DatanodeDetails dn:p.getNodes()) {
      if (datanodeHosts.contains(dn.getHostName())) {
        return true;
      }
    }
    return false;
  }

  private boolean arePipelinesOrDatanodesProvided() {
    return !(pipelineIds.equals("") && datanodes.equals(""));
  }

  private void runTest()
      throws IOException {

    timer = getMetrics().timer("chunk-write");

    byte[] data = RandomStringUtils.secure().nextAscii(chunkSize)
        .getBytes(StandardCharsets.UTF_8);

    dataToWrite = ByteString.copyFrom(data);

    Checksum checksum = new Checksum(ChecksumType.CRC32, chunkSize);
    checksumProtobuf = checksum.computeChecksum(data).getProtoBufMessage();

    runTests(this::writeChunk);
  }

  private void writeChunk(long stepNo)
      throws Exception {

    //Always use this fake blockid.
    DatanodeBlockID blockId = DatanodeBlockID.newBuilder()
        .setContainerID(1L)
        .setLocalID(stepNo % 20)
        .build();

    ChunkInfo chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(getPrefix() + "_testdata_chunk_" + stepNo)
        .setOffset((stepNo / 20) * chunkSize)
        .setLen(chunkSize)
        .setChecksumData(checksumProtobuf)
        .build();

    WriteChunkRequestProto.Builder writeChunkRequest =
        WriteChunkRequestProto
            .newBuilder()
            .setBlockID(blockId)
            .setChunkData(chunkInfo)
            .setData(dataToWrite);

    XceiverClientSpi clientSpi = xceiverClients.get(
        (int) (stepNo % (xceiverClients.size())));
    sendWriteChunkRequest(blockId, writeChunkRequest,
        clientSpi);

  }

  private void sendWriteChunkRequest(DatanodeBlockID blockId,
      WriteChunkRequestProto.Builder writeChunkRequest,
      XceiverClientSpi xceiverClientSpi) throws Exception {
    DatanodeDetails datanodeDetails = xceiverClientSpi.
        getPipeline().getFirstNode();
    String id = datanodeDetails.getUuidString();

    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto
            .newBuilder()
            .setCmdType(Type.WriteChunk)
            .setContainerID(blockId.getContainerID())
            .setDatanodeUuid(id)
            .setWriteChunk(writeChunkRequest);

    ContainerCommandRequestProto request = builder.build();
    timer.time(() -> {
      if (async) {
        XceiverClientReply xceiverClientReply =
            xceiverClientSpi.sendCommandAsync(request);
        xceiverClientSpi.watchForCommit(xceiverClientReply.getLogIndex()).get();

      } else {
        xceiverClientSpi.sendCommand(request);
      }
      return null;
    });
  }

}
