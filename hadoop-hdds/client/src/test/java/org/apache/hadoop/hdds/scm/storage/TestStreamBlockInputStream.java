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

package org.apache.hadoop.hdds.scm.storage;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.StreamingReaderSpi;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusException;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.invocation.InvocationOnMock;

/**
 * Tests for {@link TestStreamBlockInputStream}'s functionality.
 */
public class TestStreamBlockInputStream {
  private static final int BYTES_PER_CHECKSUM = 1024;
  private static final int BLOCK_SIZE = 1024;
  private StreamBlockInputStream blockStream;
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientGrpc xceiverClient;
  private Checksum checksum;
  private ChecksumData checksumData;
  private byte[] data;
  private ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver;
  private Function<BlockID, BlockLocationInfo> refreshFunction;

  @BeforeEach
  public void setup() throws Exception {
    Token<OzoneBlockTokenIdentifier> token = mock(Token.class);
    when(token.encodeToUrlString()).thenReturn("url");

    Set<HddsProtos.BlockTokenSecretProto.AccessModeProto> modes =
        Collections.singleton(HddsProtos.BlockTokenSecretProto.AccessModeProto.READ);
    OzoneBlockTokenIdentifier tokenIdentifier = new OzoneBlockTokenIdentifier("owner", new BlockID(1, 1),
        modes, Time.monotonicNow() + 10000, 10);
    tokenIdentifier.setSecretKeyId(UUID.randomUUID());
    when(token.getIdentifier()).thenReturn(tokenIdentifier.getBytes());
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();

    BlockLocationInfo blockLocationInfo = mock(BlockLocationInfo.class);
    when(blockLocationInfo.getPipeline()).thenReturn(pipeline);
    when(blockLocationInfo.getToken()).thenReturn(token);

    xceiverClient = mock(XceiverClientGrpc.class);
    when(xceiverClient.getPipeline()).thenReturn(pipeline);
    xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any()))
        .thenReturn(xceiverClient);
    requestObserver = mock(ClientCallStreamObserver.class);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamReadBlock(true);
    clientConfig.setMaxReadRetryCount(1);
    refreshFunction = mock(Function.class);
    when(refreshFunction.apply(any())).thenReturn(blockLocationInfo);
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    checksum = new Checksum(ChecksumType.CRC32, BYTES_PER_CHECKSUM);
    createDataAndChecksum();
    blockStream = new StreamBlockInputStream(blockID, BLOCK_SIZE, pipeline,
        token, xceiverClientFactory, refreshFunction, clientConfig);
  }

  @AfterEach
  public void teardown() {
    if (blockStream != null) {
      try {
        blockStream.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  @Test
  public void testCloseStreamReleasesResources() throws IOException, InterruptedException {
    setupSuccessfulRead();
    assertEquals(data[0], blockStream.read());
    blockStream.close();
    // Verify that cancel() was called on the requestObserver mock
    verify(requestObserver).cancel(any(), any());
    // Verify that release() was called on the xceiverClient mock
    verify(xceiverClientFactory).releaseClientForReadData(xceiverClient, false);
    verify(xceiverClient, times(1)).completeStreamRead(any());
  }

  @Test
  public void testUnbufferReleasesResourcesAndResumesFromLastPosition() throws IOException, InterruptedException {
    setupSuccessfulRead();
    assertEquals(data[0], blockStream.read());
    assertEquals(1, blockStream.getPos());
    blockStream.unbuffer();
    // Verify that cancel() was called on the requestObserver mock
    verify(requestObserver).cancel(any(), any());
    // Verify that release() was called on the xceiverClient mock
    verify(xceiverClientFactory).releaseClientForReadData(xceiverClient, false);
    verify(xceiverClient, times(1)).completeStreamRead(any());
    // The next read should "rebuffer" and continue from the last position
    assertEquals(data[1], blockStream.read());
    assertEquals(2, blockStream.getPos());
  }

  @Test
  public void testSeekReleasesTheStreamAndStartsFromNewPosition() throws IOException, InterruptedException {
    setupSuccessfulRead();
    assertEquals(data[0], blockStream.read());
    blockStream.seek(100);
    assertEquals(100, blockStream.getPos());
    // Verify that cancel() was called on the requestObserver mock
    verify(requestObserver).cancel(any(), any());
    verify(xceiverClient, times(1)).completeStreamRead(any());
    // The xceiverClient should not be released
    verify(xceiverClientFactory, never())
        .releaseClientForReadData(xceiverClient, false);

    assertEquals(data[100], blockStream.read());
    assertEquals(101, blockStream.getPos());
  }

  @Test
  public void testErrorThrownIfStreamReturnsError() throws IOException, InterruptedException {
    // Note the error will only be thrown when the buffer needs to be refilled. I think case, as its the first
    // read it will try to fill the buffer and encounter the error, but a reader could continue reading until the
    // buffer is exhausted before seeing the error.
    doAnswer((InvocationOnMock invocation) -> {
      StreamingReaderSpi streamObserver = invocation.getArgument(1);
      StreamingReadResponse resp =
          new StreamingReadResponse(MockDatanodeDetails.randomDatanodeDetails(), requestObserver);
      streamObserver.setStreamingReadResponse(resp);
      streamObserver.onError(new IOException("Test induced error"));
      return null;
    }).when(xceiverClient).streamRead(any(), any());
    assertThrows(IOException.class, () -> blockStream.read());
    verify(xceiverClient, times(1)).completeStreamRead(any());
  }

  @Test
  public void seekOutOfBounds() throws IOException, InterruptedException {
    setupSuccessfulRead();
    assertThrows(IOException.class, () -> blockStream.seek(-1));
    assertThrows(IOException.class, () -> blockStream.seek(BLOCK_SIZE + 1));
  }

  @Test
  public void readPastEOFReturnsEOF() throws IOException, InterruptedException {
    setupSuccessfulRead();
    blockStream.seek(BLOCK_SIZE);
    // Ensure the stream is at EOF even after two attempts to read
    assertEquals(-1, blockStream.read());
    assertEquals(-1, blockStream.read());
    assertEquals(BLOCK_SIZE, blockStream.getPos());
  }

  @Test
  public void ensureExceptionThrownForReadAfterClosed() throws IOException, InterruptedException {
    setupSuccessfulRead();
    blockStream.close();
    ByteBuffer byteBuffer = ByteBuffer.allocate(10);
    byte[] byteArray = new byte[10];
    assertThrows(IOException.class, () -> blockStream.read());
    assertThrows(IOException.class, () -> {
      // Findbugs complains about ignored return value without this :(
      int r = blockStream.read(byteArray, 0, 10);
    });
    assertThrows(IOException.class, () -> blockStream.read(byteBuffer));
    assertThrows(IOException.class, () -> blockStream.seek(10));
  }

  @ParameterizedTest
  @MethodSource("exceptionsTriggeringRefresh")
  public void testRefreshFunctionCalledForAllDNsBadOnInitialize(IOException thrown)
      throws IOException, InterruptedException {
    // In this case, if the first attempt to connect to any of the DNs fails, it should retry by refreshing the pipeline

    doAnswer((InvocationOnMock invocation) -> {
      throw thrown;
    }).doAnswer((InvocationOnMock invocation) -> {
      StreamingReaderSpi streamObserver = invocation.getArgument(1);
      StreamingReadResponse resp =
          new StreamingReadResponse(MockDatanodeDetails.randomDatanodeDetails(), requestObserver);
      streamObserver.setStreamingReadResponse(resp);
      streamObserver.onNext(createChunkResponse(false));
      streamObserver.onCompleted();
      return null;
    }).when(xceiverClient).streamRead(any(), any());
    blockStream.read();
    verify(refreshFunction, times(1)).apply(any());
  }

  @ParameterizedTest
  @MethodSource("exceptionsNotTriggeringRefresh")
  public void testRefreshNotCalledForAllDNsBadOnInitialize(IOException thrown)
      throws IOException, InterruptedException {
    // In this case, if the first attempt to connect to any of the DNs fails, it should retry by refreshing the pipeline
    doAnswer((InvocationOnMock invocation) -> {
      throw thrown;
    }).when(xceiverClient).streamRead(any(), any());
    assertThrows(IOException.class, () -> blockStream.read());
    verify(refreshFunction, times(0)).apply(any());
  }

  @Test
  public void testExceptionThrownAfterRetriesExhausted() throws IOException, InterruptedException {
    // In this case, if the first attempt to connect to any of the DNs fails, it should retry by refreshing the pipeline
    doAnswer((InvocationOnMock invocation) -> {
      throw new StorageContainerException(CONTAINER_NOT_FOUND);
    }).when(xceiverClient).streamRead(any(), any());

    assertThrows(IOException.class, () -> blockStream.read());
    verify(refreshFunction, times(1)).apply(any());
  }

  @Test
  public void testInvalidChecksumThrowsException() throws IOException, InterruptedException {
    doAnswer((InvocationOnMock invocation) -> {
      StreamingReaderSpi streamObserver = invocation.getArgument(1);
      StreamingReadResponse resp =
          new StreamingReadResponse(MockDatanodeDetails.randomDatanodeDetails(), requestObserver);
      streamObserver.setStreamingReadResponse(resp);
      streamObserver.onNext(createChunkResponse(true));
      streamObserver.onCompleted();
      return null;
    }).when(xceiverClient).streamRead(any(), any());
    assertThrows(IOException.class, () -> blockStream.read());
  }

  private void createDataAndChecksum() throws OzoneChecksumException {
    data = new byte[BLOCK_SIZE];
    new SecureRandom().nextBytes(data);
    checksumData = checksum.computeChecksum(data);
  }

  private void setupSuccessfulRead() throws IOException, InterruptedException {
    doAnswer((InvocationOnMock invocation) -> {
      StreamingReaderSpi streamObserver = invocation.getArgument(1);
      StreamingReadResponse resp =
          new StreamingReadResponse(MockDatanodeDetails.randomDatanodeDetails(), requestObserver);
      streamObserver.setStreamingReadResponse(resp);
      streamObserver.onNext(createChunkResponse(false));
      streamObserver.onCompleted();
      return null;
    }).when(xceiverClient).streamRead(any(), any());
  }

  private ContainerProtos.ContainerCommandResponseProto createChunkResponse(boolean invalidChecksum) {
    ContainerProtos.ReadBlockResponseProto response = invalidChecksum ?
        createInValidChecksumResponse() : createValidResponse();

    return ContainerProtos.ContainerCommandResponseProto.newBuilder()
        .setCmdType(ContainerProtos.Type.ReadBlock)
        .setReadBlock(response)
        .setResult(ContainerProtos.Result.SUCCESS)
        .build();
  }

  private ContainerProtos.ReadBlockResponseProto createValidResponse() {
    return ContainerProtos.ReadBlockResponseProto.newBuilder()
        .setChecksumData(checksumData.getProtoBufMessage())
        .setData(ByteString.copyFrom(data))
        .setOffset(0)
        .build();
  }

  private ContainerProtos.ReadBlockResponseProto createInValidChecksumResponse() {
    byte[] invalidData = new byte[data.length];
    System.arraycopy(data, 0, invalidData, 0, data.length);
    // Corrupt the data
    invalidData[0] = (byte) (invalidData[0] + 1);
    return ContainerProtos.ReadBlockResponseProto.newBuilder()
        .setChecksumData(checksumData.getProtoBufMessage())
        .setData(ByteString.copyFrom(invalidData))
        .setOffset(0)
        .build();
  }

  private static Stream<Arguments> exceptionsTriggeringRefresh() {
    return Stream.of(
        Arguments.of(new StorageContainerException(CONTAINER_NOT_FOUND)),
        Arguments.of(new IOException(new ExecutionException(
            new StatusException(Status.UNAVAILABLE))))
    );
  }

  private static Stream<Arguments> exceptionsNotTriggeringRefresh() {
    return Stream.of(
        Arguments.of(new SCMSecurityException("Security problem")),
        Arguments.of(new OzoneChecksumException("checksum missing")),
        Arguments.of(new IOException("Some random exception."))
    );
  }

}
