/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.protocolPB;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto.Builder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DataBuffers;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkVersion;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import static org.apache.hadoop.hdds.scm.utils.ClientCommandsUtils.getReadChunkVersion;

/**
 * A set of helper functions to create responses to container commands.
 */
public final class ContainerCommandResponseBuilders {

  /**
   * Returns a Container Command Response Builder with the specified result
   * and message.
   * @param request requestProto message.
   * @param result result of the command.
   * @param message response message.
   * @return ContainerCommand Response Builder.
   */
  public static Builder getContainerCommandResponse(
      ContainerCommandRequestProto request, Result result, String message) {

    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(request.getCmdType())
        .setTraceID(request.getTraceID())
        .setResult(result)
        .setMessage(message);
  }

  /**
   * Returns a Container Command Response Builder. This call is used to build
   * success responses. Calling function can add other fields to the response
   * as required.
   * @param request requestProto message.
   * @return ContainerCommand Response Builder with result as SUCCESS.
   */
  public static Builder getSuccessResponseBuilder(
      ContainerCommandRequestProto request) {

    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(request.getCmdType())
        .setTraceID(request.getTraceID())
        .setResult(Result.SUCCESS);
  }

  /**
   * Returns a Container Command Response. This call is used for creating null
   * success responses.
   * @param request requestProto message.
   * @return ContainerCommand Response with result as SUCCESS.
   */
  public static ContainerCommandResponseProto getSuccessResponse(
      ContainerCommandRequestProto request) {

    return getSuccessResponseBuilder(request)
        .setMessage("")
        .build();
  }

  /**
   * We found a command type but no associated payload for the command. Hence
   * return malformed Command as response.
   *
   * @param request - Protobuf message.
   * @return ContainerCommandResponseProto - MALFORMED_REQUEST.
   */
  public static ContainerCommandResponseProto malformedRequest(
      ContainerCommandRequestProto request) {

    return getContainerCommandResponse(request, Result.MALFORMED_REQUEST,
        "Cmd type does not match the payload.")
        .build();
  }

  /**
   * We found a command type that is not supported yet.
   *
   * @param request - Protobuf message.
   * @return ContainerCommandResponseProto - UNSUPPORTED_REQUEST.
   */
  public static ContainerCommandResponseProto unsupportedRequest(
      ContainerCommandRequestProto request) {

    return getContainerCommandResponse(request, Result.UNSUPPORTED_REQUEST,
        "Server does not support this command yet.")
        .build();
  }

  /**
   * Returns putBlock response success.
   * @param msg - Request.
   * @return Response.
   */
  public static ContainerCommandResponseProto putBlockResponseSuccess(
      ContainerCommandRequestProto msg, BlockData blockData) {

    PutBlockResponseProto.Builder putBlock = PutBlockResponseProto.newBuilder()
        .setCommittedBlockLength(getCommittedBlockLengthResponseBuilder(
            blockData.getSize(), blockData.getBlockID()));

    return getSuccessResponseBuilder(msg)
        .setPutBlock(putBlock)
        .build();
  }

  /**
   * Returns successful blockResponse.
   * @param msg - Request.
   * @return Response.
   */
  public static ContainerCommandResponseProto getBlockResponseSuccess(
      ContainerCommandRequestProto msg) {

    return getSuccessResponse(msg);
  }

  public static ContainerCommandResponseProto getBlockDataResponse(
      ContainerCommandRequestProto msg, BlockData data) {

    GetBlockResponseProto.Builder getBlock = GetBlockResponseProto.newBuilder()
        .setBlockData(data);

    return getSuccessResponseBuilder(msg)
        .setGetBlock(getBlock)
        .build();
  }

  /**
   * Returns successful getCommittedBlockLength Response.
   * @param msg - Request.
   * @return Response.
   */
  public static ContainerCommandResponseProto getBlockLengthResponse(
      ContainerCommandRequestProto msg, long blockLength) {

    GetCommittedBlockLengthResponseProto.Builder committedBlockLength =
        getCommittedBlockLengthResponseBuilder(blockLength,
            msg.getGetCommittedBlockLength().getBlockID());

    return getSuccessResponseBuilder(msg)
        .setGetCommittedBlockLength(committedBlockLength)
        .build();
  }

  public static GetCommittedBlockLengthResponseProto.Builder
      getCommittedBlockLengthResponseBuilder(long blockLength,
      DatanodeBlockID blockID) {

    return GetCommittedBlockLengthResponseProto.newBuilder()
        .setBlockLength(blockLength)
        .setBlockID(blockID);
  }

  /**
   * Gets a response for the putSmallFile RPC.
   * @param msg - ContainerCommandRequestProto
   * @return - ContainerCommandResponseProto
   */
  public static ContainerCommandResponseProto getPutFileResponseSuccess(
      ContainerCommandRequestProto msg, BlockData blockData) {

    PutSmallFileResponseProto.Builder putSmallFile =
        PutSmallFileResponseProto.newBuilder()
            .setCommittedBlockLength(getCommittedBlockLengthResponseBuilder(
                blockData.getSize(), blockData.getBlockID()));

    return getSuccessResponseBuilder(msg)
        .setCmdType(Type.PutSmallFile)
        .setPutSmallFile(putSmallFile)
        .build();
  }

  /**
   * Gets a response to the read small file call.
   * @param request - Msg
   * @param dataBuffers  - Data
   * @param info  - Info
   * @return    Response.
   */
  public static ContainerCommandResponseProto getGetSmallFileResponseSuccess(
      ContainerCommandRequestProto request, List<ByteString> dataBuffers,
      ChunkInfo info) {

    Preconditions.checkNotNull(request);

    boolean isReadChunkV0 = getReadChunkVersion(request.getGetSmallFile())
        .equals(ContainerProtos.ReadChunkVersion.V0);

    ReadChunkResponseProto.Builder readChunk;

    if (isReadChunkV0) {
      // V0 has all response data in a single ByteBuffer
      ByteString combinedData = ByteString.EMPTY;
      for (ByteString buffer : dataBuffers) {
        combinedData.concat(buffer);
      }
      readChunk = ReadChunkResponseProto.newBuilder()
          .setChunkData(info)
          .setData(combinedData)
          .setBlockID(request.getGetSmallFile().getBlock().getBlockID());
    } else {
      // V1 splits response data into a list of ByteBuffers
      readChunk = ReadChunkResponseProto.newBuilder()
          .setChunkData(info)
          .setDataBuffers(DataBuffers.newBuilder()
              .addAllBuffers(dataBuffers)
              .build())
          .setBlockID(request.getGetSmallFile().getBlock().getBlockID());
    }

    GetSmallFileResponseProto.Builder getSmallFile =
        GetSmallFileResponseProto.newBuilder().setData(readChunk);

    return getSuccessResponseBuilder(request)
        .setCmdType(Type.GetSmallFile)
        .setGetSmallFile(getSmallFile)
        .build();
  }
  /**
   * Returns a ReadContainer Response.
   *
   * @param request Request
   * @param containerData - data
   * @return Response.
   */
  public static ContainerCommandResponseProto getReadContainerResponse(
      ContainerCommandRequestProto request, ContainerDataProto containerData) {

    Preconditions.checkNotNull(containerData);

    ReadContainerResponseProto.Builder response =
        ReadContainerResponseProto.newBuilder()
            .setContainerData(containerData);

    return getSuccessResponseBuilder(request)
        .setReadContainer(response)
        .build();
  }

  public static ContainerCommandResponseProto getReadChunkResponse(
      ContainerCommandRequestProto request, ChunkBuffer data,
      Function<ByteBuffer, ByteString> byteBufferToByteString) {

    boolean isReadChunkV0 = getReadChunkVersion(request.getReadChunk())
        .equals(ContainerProtos.ReadChunkVersion.V0);

    ReadChunkResponseProto.Builder response;

    if (isReadChunkV0) {
      // V0 has all response data in a single ByteBuffer
      response = ReadChunkResponseProto.newBuilder()
          .setChunkData(request.getReadChunk().getChunkData())
          .setData(data.toByteString(byteBufferToByteString))
          .setBlockID(request.getReadChunk().getBlockID());
    } else {
      // V1 splits response data into a list of ByteBuffers
      List<ByteBuffer> byteBuffers = data.asByteBufferList();
      response = ReadChunkResponseProto.newBuilder()
          .setChunkData(request.getReadChunk().getChunkData())
          .setDataBuffers(DataBuffers.newBuilder()
              .addAllBuffers(data.toByteStringList(byteBufferToByteString))
              .build())
          .setBlockID(request.getReadChunk().getBlockID());
    }

    return getSuccessResponseBuilder(request)
        .setReadChunk(response)
        .build();
  }

  private ContainerCommandResponseBuilders() {
    throw new UnsupportedOperationException("no instances");
  }
}
