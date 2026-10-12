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

package org.apache.hadoop.ozone.client.checksum;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType.CRC32;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType.CRC32C;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.ChecksumCombineMode.COMPOSITE_CRC;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.client.MockOmTransport;
import org.apache.hadoop.ozone.client.MockXceiverClientFactory;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.PureJavaCrc32C;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.event.Level;

/**
 * Unit tests for Replicated and EC FileChecksumHelper class.
 */
public class TestFileChecksumHelper {
  private static final int BYTES_PER_CRC = 512;
  private static final byte[] EMPTY_BLOCK = new byte[0];

  private final FileChecksum noCachedChecksum = null;
  private OzoneClient client;
  private ObjectStore store;
  private OzoneVolume volume;
  private RpcClient rpcClient;

  @BeforeEach
  public void init() throws IOException {
    InMemoryConfigurationForTesting config = new InMemoryConfigurationForTesting();
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.CRC32C);

    config.setFromObject(clientConfig);

    rpcClient = new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(
          String omServiceId) {
        return new MockOmTransport();
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    };

    client = new OzoneClient(config, rpcClient);

    store = client.getObjectStore();
  }

  @AfterEach
  public void close() throws IOException {
    client.close();
  }

  private OmKeyInfo omKeyInfo(ReplicationType type, FileChecksum cachedChecksum,
      List<OmKeyLocationInfo> locationInfo) {
    return omKeyInfo(type, cachedChecksum, locationInfo, 0);
  }

  private OmKeyInfo omKeyInfo(ReplicationType type, FileChecksum cachedChecksum,
      List<OmKeyLocationInfo> locationInfo, long dataSize) {
    ReplicationConfig config = type == ReplicationType.EC ? new ECReplicationConfig(6, 3)
        : RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);

    return new OmKeyInfo.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket")
        .setKeyName("key")
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locationInfo)))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(dataSize)
        .setReplicationConfig(config)
        .setFileEncryptionInfo(null)
        .setFileChecksum(cachedChecksum)
        .setAcls(null)
        .build();
  }

  /**
   * Feeds block checksums the way datanodes return them, without talking to
   * any datanode. For COMPOSITE_CRC a non-empty block yields a 4-byte CRC,
   * while an empty block yields no bytes at all.
   */
  private static class FakeChecksumComputer extends BaseFileChecksumHelper {
    private final List<byte[]> blockChecksums;

    FakeChecksumComputer(OzoneVolume volume, OzoneBucket bucket, OmKeyInfo keyInfo,
        List<byte[]> blockChecksums, OzoneClientConfig.ChecksumCombineMode mode,
        RpcClient rpcClient) throws IOException {
      super(volume, bucket, keyInfo.getKeyName(), Long.MAX_VALUE, mode, rpcClient, keyInfo);
      this.blockChecksums = blockChecksums;
    }

    @Override
    protected AbstractBlockChecksumComputer getBlockChecksumComputer(
        List<ContainerProtos.ChunkInfo> chunkInfos, long blockLength) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected List<ContainerProtos.ChunkInfo> getChunkInfos(
        OmKeyLocationInfo keyLocationInfo) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void checksumBlocks() throws IOException {
      if (getCombineMode() == COMPOSITE_CRC) {
        setChecksumType(CRC32C);
        setBytesPerCRC(BYTES_PER_CRC);
      } else {
        setChecksumType(CRC32);
        setBytesPerCRC(BYTES_PER_CRC);
      }
      for (byte[] blockChecksum : blockChecksums) {
        getBlockChecksumBuf().write(blockChecksum);
      }
    }

    String populateCompositeBlockChecksumForDebug(ByteBuffer blockChecksumByteBuffer)
        throws IOException {
      return populateBlockChecksumBuf(blockChecksumByteBuffer);
    }
  }

  private static RpcClient mockRpcClientForChecksumHelper() {
    RpcClient mockRpcClient = mock(RpcClient.class);
    when(mockRpcClient.getXceiverClientManager()).thenReturn(mock(XceiverClientFactory.class));
    return mockRpcClient;
  }

  private static OzoneVolume mockVolume() {
    OzoneVolume mockVolume = mock(OzoneVolume.class);
    when(mockVolume.getName()).thenReturn("vol1");
    return mockVolume;
  }

  private static OzoneBucket mockBucket() {
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.getName()).thenReturn("bucket");
    return mockBucket;
  }

  private static byte[] randomBytes(int len, long seed) {
    byte[] data = new byte[len];
    new Random(seed).nextBytes(data);
    return data;
  }

  private static int crc32c(byte[]... parts) {
    PureJavaCrc32C crc = new PureJavaCrc32C();
    for (byte[] part : parts) {
      crc.update(part, 0, part.length);
    }
    return (int) crc.getValue();
  }

  private static OmKeyLocationInfo blockLocation(long localId, long length) {
    return new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, localId))
        .setLength(length)
        .build();
  }

  private static List<OmKeyLocationInfo> locationsFor(byte[]... blocks) {
    List<OmKeyLocationInfo> located = new ArrayList<>();
    for (int i = 0; i < blocks.length; i++) {
      located.add(blockLocation(i + 1, blocks[i].length));
    }
    return located;
  }

  private static List<byte[]> compositeBlockChecksums(byte[]... blocks) {
    List<byte[]> checksums = new ArrayList<>();
    for (byte[] block : blocks) {
      checksums.add(block.length == 0
          ? EMPTY_BLOCK : CrcUtil.intToBytes(crc32c(block)));
    }
    return checksums;
  }

  private FileChecksum compositeChecksum(ReplicationType type, byte[]... blocks)
      throws IOException {
    long fileLength = 0;
    for (byte[] block : blocks) {
      fileLength += block.length;
    }
    List<OmKeyLocationInfo> located = locationsFor(blocks);
    FakeChecksumComputer computer = new FakeChecksumComputer(
        mockVolume(), mockBucket(),
        omKeyInfo(type, noCachedChecksum, located, fileLength),
        compositeBlockChecksums(blocks), COMPOSITE_CRC, mockRpcClientForChecksumHelper());
    computer.compute();
    return computer.getFileChecksum();
  }

  private static FileChecksum expectedComposite(byte[]... data) {
    return new CompositeCrcFileChecksum(
        crc32c(data), DataChecksum.Type.CRC32C, BYTES_PER_CRC);
  }

  private BaseFileChecksumHelper checksumHelper(ReplicationType type, OzoneVolume mockVolume, OzoneBucket mockBucket,
      int length, OzoneClientConfig.ChecksumCombineMode combineMode, RpcClient mockRpcClient, OmKeyInfo keyInfo)
      throws IOException {
    return type == ReplicationType.RATIS ? new ReplicatedFileChecksumHelper(
        mockVolume, mockBucket, "dummy", length, combineMode, mockRpcClient, keyInfo)
        : new ECFileChecksumHelper(
        mockVolume, mockBucket, "dummy", length, combineMode, mockRpcClient, keyInfo);
  }

  private Pipeline pipeline(ReplicationType type, List<DatanodeDetails> datanodeDetails) {
    ReplicationConfig config = type == ReplicationType.RATIS ? RatisReplicationConfig
        .getInstance(HddsProtos.ReplicationFactor.THREE)
        : new ECReplicationConfig(6, 3);

    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(config)
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(datanodeDetails)
        .build();
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testCompositeCrcOfNonEmptyBlocks(ReplicationType helperType)
      throws IOException {
    byte[] a = randomBytes(1000, 1);
    byte[] b = randomBytes(700, 2);
    assertEquals(expectedComposite(a), compositeChecksum(helperType, a));
    assertEquals(expectedComposite(a, b), compositeChecksum(helperType, a, b));
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testCompositeCrcOfZeroLengthKeyWithEmptyBlock(ReplicationType helperType)
      throws IOException {
    assertEquals(compositeChecksum(helperType),
        compositeChecksum(helperType, EMPTY_BLOCK));
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testCompositeCrcSkipsTrailingEmptyBlock(ReplicationType helperType)
      throws IOException {
    byte[] a = randomBytes(1000, 3);
    assertEquals(expectedComposite(a), compositeChecksum(helperType, a, EMPTY_BLOCK));
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testCompositeCrcSkipsEmptyBlockInTheMiddle(ReplicationType helperType)
      throws IOException {
    byte[] a = randomBytes(1000, 4);
    byte[] b = randomBytes(700, 5);
    assertEquals(expectedComposite(a, b),
        compositeChecksum(helperType, a, EMPTY_BLOCK, b));
  }

  @Test
  public void testMd5CrcOfZeroLengthKeyWithEmptyBlockIsUnchanged() throws IOException {
    List<OmKeyLocationInfo> located =
        Collections.singletonList(blockLocation(1, 0));
    List<byte[]> checksums = new ArrayList<>();
    // An MD5CRC block checksum is always a 16-byte MD5, even for empty blocks.
    checksums.add(new byte[16]);
    FakeChecksumComputer computer = new FakeChecksumComputer(
        mockVolume(), mockBucket(),
        omKeyInfo(ReplicationType.RATIS, noCachedChecksum, located),
        checksums, MD5MD5CRC, mockRpcClientForChecksumHelper());
    computer.compute();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, computer.getFileChecksum());
  }

  @Test
  public void testPopulateEmptyCompositeBlockChecksumWithDebugLogging()
      throws IOException {
    GenericTestUtils.setLogLevel(BaseFileChecksumHelper.class, Level.DEBUG);
    FakeChecksumComputer computer = new FakeChecksumComputer(
        mockVolume(), mockBucket(),
        omKeyInfo(ReplicationType.RATIS, noCachedChecksum, new ArrayList<>()),
        new ArrayList<>(), COMPOSITE_CRC, mockRpcClientForChecksumHelper());
    assertNotEquals(null, computer.populateCompositeBlockChecksumForDebug(
        ByteBuffer.allocate(0)));
    assertEquals(0, computer.getBlockChecksumBuf().getLength());
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testEmptyBlock(ReplicationType helperType) throws IOException {
    // test the file checksum of a file with an empty block.
    RpcClient mockRpcClient = mock(RpcClient.class);
    OmKeyInfo omKeyInfo = omKeyInfo(helperType, noCachedChecksum, new ArrayList<>());
    OzoneVolume mockVolume = mock(OzoneVolume.class);
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(om);
    when(om.lookupKey(any())).thenReturn(omKeyInfo);
    when(mockVolume.getName()).thenReturn("vol1");
    when(mockBucket.getName()).thenReturn("bucket1");


    BaseFileChecksumHelper helper =
        checksumHelper(helperType, mockVolume, mockBucket, 10, combineMode, mockRpcClient, omKeyInfo);
    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(DataChecksum.Type.CRC32,
        ((MD5MD5CRC32GzipFileChecksum) fileChecksum).getCrcType());

    // test negative length
    helper =
        checksumHelper(helperType, mockVolume, mockBucket, -1, combineMode, mockRpcClient, omKeyInfo);
    helper.compute();
    assertNull(helper.getKeyLocationInfoList());
  }

  @ParameterizedTest
  @EnumSource(names = {"EC", "RATIS"})
  public void testOneBlock(ReplicationType helperType) throws IOException {
    // test the file checksum of a file with one block.
    OzoneConfiguration conf = new OzoneConfiguration();
    RpcClient mockRpcClient = mock(RpcClient.class);
    List<DatanodeDetails> dns = Collections.singletonList(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build());
    Pipeline pipeline = pipeline(helperType, dns);
    BlockID blockID = new BlockID(1, 1);
    OmKeyLocationInfo omKeyLocationInfo =
        new OmKeyLocationInfo.Builder()
            .setPipeline(pipeline)
            .setBlockID(blockID)
            .build();
    List<OmKeyLocationInfo> omKeyLocationInfoList =
        Collections.singletonList(omKeyLocationInfo);
    OmKeyInfo omKeyInfo = omKeyInfo(helperType, noCachedChecksum, omKeyLocationInfoList);
    XceiverClientGrpc xceiverClientGrpc =
        new XceiverClientGrpc(pipeline, conf) {
          @Override
          public XceiverClientReply sendCommandAsync(
              ContainerProtos.ContainerCommandRequestProto request,
              DatanodeDetails dn) {
            return buildValidResponse(helperType);
          }
        };
    XceiverClientFactory factory = mock(XceiverClientFactory.class);
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    when(factory.acquireClientForReadData(any())).
        thenReturn(xceiverClientGrpc);
    when(mockRpcClient.getXceiverClientManager()).thenReturn(factory);
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(om);
    when(om.lookupKey(any())).thenReturn(omKeyInfo);

    OzoneVolume mockVolume = mock(OzoneVolume.class);
    when(mockVolume.getName()).thenReturn("vol1");
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.getName()).thenReturn("bucket1");

    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

    BaseFileChecksumHelper helper = checksumHelper(
        helperType, mockVolume, mockBucket, 10, combineMode, mockRpcClient, omKeyInfo);

    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(1, helper.getKeyLocationInfoList().size());

    FileChecksum cachedChecksum = new MD5MD5CRC32GzipFileChecksum();
    /// test cached checksum
    OmKeyInfo omKeyInfoWithChecksum = omKeyInfo(helperType, cachedChecksum, omKeyLocationInfoList);
    when(om.lookupKey(any())).
        thenReturn(omKeyInfoWithChecksum);

    helper = checksumHelper(
        helperType, mockVolume, mockBucket, 10, combineMode, mockRpcClient, omKeyInfo);

    helper.compute();
    fileChecksum = helper.getFileChecksum();
    assertInstanceOf(MD5MD5CRC32GzipFileChecksum.class, fileChecksum);
    assertEquals(1, helper.getKeyLocationInfoList().size());
  }

  private XceiverClientReply buildValidResponse(ReplicationType type) {
    // return a GetBlockResponse message of a block and its chunk checksums.
    ContainerProtos.DatanodeBlockID blockID =
        ContainerProtos.DatanodeBlockID.newBuilder()
        .setContainerID(1)
        .setLocalID(1)
        .setBlockCommitSequenceId(1).build();

    byte[] byteArray = new byte[12];
    ByteString byteString = ByteString.copyFrom(byteArray);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
        .setType(CRC32)
        .setBytesPerChecksum(1024)
        .addChecksums(byteString)
        .build();

    ContainerProtos.ChunkInfo.Builder chunkInfoBuilder = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName("dummy_chunk")
        .setOffset(1)
        .setLen(10)
        .setChecksumData(checksumData);

    if (type == ReplicationType.EC) {
      chunkInfoBuilder.setStripeChecksum(byteString);
    }

    ContainerProtos.ChunkInfo chunkInfo = chunkInfoBuilder.build();

    ContainerProtos.BlockData blockData =
        ContainerProtos.BlockData.newBuilder()
            .setBlockID(blockID)
            .addChunks(chunkInfo)
            .build();
    ContainerProtos.GetBlockResponseProto getBlockResponseProto
        = ContainerProtos.GetBlockResponseProto.newBuilder()
        .setBlockData(blockData)
        .build();

    ContainerProtos.ContainerCommandResponseProto resp =
        ContainerProtos.ContainerCommandResponseProto.newBuilder()
            .setCmdType(ContainerProtos.Type.GetBlock)
            .setResult(ContainerProtos.Result.SUCCESS)
            .setGetBlock(getBlockResponseProto)
            .build();
    final CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
        replyFuture = new CompletableFuture<>();
    replyFuture.complete(resp);
    return new XceiverClientReply(replyFuture);
  }

  private OzoneBucket getOzoneBucket() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    return volume.getBucket(bucketName);
  }

  /**
   * Write a real key and compute file checksum of it.
   *
   * @throws IOException
   */
  @Test
  public void testPutKeyChecksum() throws IOException {
    String value = new String(new byte[1024], UTF_8);
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 1; i++) {
      String keyName = UUID.randomUUID().toString();

      try (OzoneOutputStream out = bucket
          .createKey(keyName, value.getBytes(UTF_8).length,
              ReplicationType.RATIS, ONE, new HashMap<>())) {
        out.write(value.getBytes(UTF_8));
        out.write(value.getBytes(UTF_8));
      }

      OzoneClientConfig.ChecksumCombineMode combineMode =
          OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

      OmKeyInfo keyInfo = rpcClient.getKeyInfo(
          volume.getName(), bucket.getName(), keyName, false);
      ReplicatedFileChecksumHelper helper = new ReplicatedFileChecksumHelper(
          volume, bucket, keyName, 10, combineMode, rpcClient, keyInfo);

      helper.compute();
      FileChecksum fileChecksum = helper.getFileChecksum();
      assertEquals(DataChecksum.Type.CRC32C,
          ((MD5MD5CRC32FileChecksum)fileChecksum).getCrcType());
      assertEquals(1, helper.getKeyLocationInfoList().size());
    }
  }
}
