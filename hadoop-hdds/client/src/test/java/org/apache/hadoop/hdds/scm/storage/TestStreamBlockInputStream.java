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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  @BeforeEach
  public void setup() throws Exception {
    Token<OzoneBlockTokenIdentifier> token = mock(Token.class);
    when(token.encodeToUrlString()).thenReturn("url");
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    xceiverClient = mock(XceiverClientGrpc.class);
    when(xceiverClient.getPipeline()).thenReturn(pipeline);
    xceiverClientFactory = mock(XceiverClientFactory.class);
    when(xceiverClientFactory.acquireClientForReadData(any()))
        .thenReturn(xceiverClient);
    requestObserver = mock(ClientCallStreamObserver.class);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamReadBlock(true);
    Function<BlockID, BlockLocationInfo> refreshFunction = mock(Function.class);
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
  public void testCloseStreamReleasesResources() throws IOException {
    setupSuccessfulRead();
    assertEquals(data[0], blockStream.read());
    blockStream.close();
    // Verify that cancel() was called on the requestObserver mock
    org.mockito.Mockito.verify(requestObserver).cancel(any(), any());
    // Verify that release() was called on the xceiverClient mock
    org.mockito.Mockito.verify(xceiverClientFactory).releaseClientForReadData(xceiverClient, false);
  }

  @Test
  public void testUnbufferReleasesResourcesAndResumesFromLastPosition() throws IOException {
    setupSuccessfulRead();
    assertEquals(data[0], blockStream.read());
    assertEquals(1, blockStream.getPos());
    blockStream.unbuffer();
    // Verify that cancel() was called on the requestObserver mock
    org.mockito.Mockito.verify(requestObserver).cancel(any(), any());
    // Verify that release() was called on the xceiverClient mock
    org.mockito.Mockito.verify(xceiverClientFactory).releaseClientForReadData(xceiverClient, false);
    // The next read should "rebuffer" and continue from the last position
    assertEquals(data[1], blockStream.read());
    assertEquals(2, blockStream.getPos());
  }

  @Test
  public void testSeekReleasesTheStreamAndStartsFromNewPosition() throws IOException {
    setupSuccessfulRead();
    assertEquals(data[0], blockStream.read());
    blockStream.seek(100);
    assertEquals(100, blockStream.getPos());
    // Verify that cancel() was called on the requestObserver mock
    org.mockito.Mockito.verify(requestObserver).cancel(any(), any());
    // The xceiverClient should not be released
    org.mockito.Mockito.verify(xceiverClientFactory, never())
        .releaseClientForReadData(xceiverClient, false);

    assertEquals(data[100], blockStream.read());
    assertEquals(101, blockStream.getPos());
  }

  @Test
  public void testErrorThrownIfStreamReturnsError() throws IOException {
    // Note the error will only be thrown when the buffer needs to be refilled. I think case, as its the first
    // read it will try to fill the buffer and encounter the error, but a reader could continue reading until the
    // buffer is exhausted before seeing the error.
    when(xceiverClient.streamRead(any(), any())).thenAnswer((InvocationOnMock invocation) -> {
      StreamObserver<ContainerProtos.ContainerCommandResponseProto> streamObserver = invocation.getArgument(1);
      streamObserver.onError(new IOException("Test induced error"));
      return requestObserver;
    });
    assertThrows(IOException.class, () -> blockStream.read());
  }

  @Test
  public void seekOutOfBounds() throws IOException {
    setupSuccessfulRead();
    assertThrows(IOException.class, () -> blockStream.seek(-1));
    assertThrows(IOException.class, () -> blockStream.seek(BLOCK_SIZE + 1));
  }

  @Test
  public void readPastEOFReturnsEOF() throws IOException {
    setupSuccessfulRead();
    blockStream.seek(BLOCK_SIZE);
    // Ensure the stream is at EOF even after two attempts to read
    assertEquals(-1, blockStream.read());
    assertEquals(-1, blockStream.read());
    assertEquals(BLOCK_SIZE, blockStream.getPos());
  }

  @Test
  public void ensureExceptionThrownForReadAfterClosed() throws IOException {
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

  private void createDataAndChecksum() throws OzoneChecksumException {
    data = new byte[BLOCK_SIZE];
    new SecureRandom().nextBytes(data);
    checksumData = checksum.computeChecksum(data);
  }

  private void setupSuccessfulRead() throws IOException {
    when(xceiverClient.streamRead(any(), any())).thenAnswer((InvocationOnMock invocation) -> {
      StreamObserver<ContainerProtos.ContainerCommandResponseProto> streamObserver = invocation.getArgument(1);
      streamObserver.onNext(createChunkResponse());
      streamObserver.onCompleted();
      return requestObserver;
    });
  }

  private ContainerProtos.ContainerCommandResponseProto createChunkResponse() {
    ContainerProtos.ReadBlockResponseProto response = ContainerProtos.ReadBlockResponseProto.newBuilder()
        .setChecksumData(checksumData.getProtoBufMessage())
        .setData(ByteString.copyFrom(data))
        .setOffset(0)
        .build();

    return ContainerProtos.ContainerCommandResponseProto.newBuilder()
        .setCmdType(ContainerProtos.Type.ReadBlock)
        .setReadBlock(response)
        .setResult(ContainerProtos.Result.SUCCESS)
        .build();
  }

}
