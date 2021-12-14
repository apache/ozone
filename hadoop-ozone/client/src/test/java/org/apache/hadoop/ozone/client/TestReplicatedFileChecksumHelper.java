package org.apache.hadoop.ozone.client;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
//import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestReplicatedFileChecksumHelper {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void compute() {
  }

  @Test
  public void testEmptyBlock() throws IOException {
    RpcClient rpcClient = Mockito.mock(RpcClient.class);
    XceiverClientGrpc xceiverClientGrpc = Mockito.mock(XceiverClientGrpc.class);

    OzoneManagerProtocol om = Mockito.mock(OzoneManagerProtocol.class);
    when(rpcClient.getOzoneManagerClient()).thenReturn(om);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(null)
        .setBucketName(null)
        .setKeyName(null)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(new RatisReplicationConfig(
            HddsProtos.ReplicationFactor.ONE))
        .setFileEncryptionInfo(null)
        .setAcls(null)
        .build();

    when(om.lookupKey(any())).thenReturn(omKeyInfo);

    OzoneVolume volume = Mockito.mock(OzoneVolume.class);
    when(volume.getName()).thenReturn("vol1");
    OzoneBucket bucket = Mockito.mock(OzoneBucket.class);
    when(bucket.getName()).thenReturn("bucket1");

    ReplicatedFileChecksumHelper helper = new ReplicatedFileChecksumHelper(
        volume, bucket, "dummy", 10, rpcClient, xceiverClientGrpc);
    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertTrue(fileChecksum instanceof MD5MD5CRC32GzipFileChecksum);
    assertEquals(DataChecksum.Type.CRC32,
        ((MD5MD5CRC32GzipFileChecksum)fileChecksum).getCrcType());
  }

  @Test
  public void testOneBlock() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();

    RpcClient rpcClient = Mockito.mock(RpcClient.class);
    //XceiverClientGrpc xceiverClientGrpc = Mockito.mock(XceiverClientGrpc.class);

    List<DatanodeDetails> dns;
    dns = new ArrayList<>();
    //dns.add(MockDatanodeDetails.randomDatanodeDetails());
    Pipeline pipeline;
    pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            new RatisReplicationConfig(HddsProtos.ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(dns)
        .build();

    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf) {
      @Override
      public XceiverClientReply sendCommandAsync(
          ContainerProtos.ContainerCommandRequestProto request,
          DatanodeDetails dn) {
        //allDNs.remove(dn);
        return buildValidResponse();
      }
    };
    //Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    //XceiverClientFactory factory = new MockXceiverClientFactory();
    //XceiverClientSpi client = factory.acquireClient(pipeline);



    OzoneManagerProtocol om = Mockito.mock(OzoneManagerProtocol.class);
    when(rpcClient.getOzoneManagerClient()).thenReturn(om);

    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>(1);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(null)
        .setBucketName(null)
        .setKeyName(null)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>(omKeyLocationInfoList))))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(new RatisReplicationConfig(
            HddsProtos.ReplicationFactor.ONE))
        .setFileEncryptionInfo(null)
        .setAcls(null)
        .build();

    when(om.lookupKey(any())).thenReturn(omKeyInfo);

    OzoneVolume volume = Mockito.mock(OzoneVolume.class);
    when(volume.getName()).thenReturn("vol1");
    OzoneBucket bucket = Mockito.mock(OzoneBucket.class);
    when(bucket.getName()).thenReturn("bucket1");

    ReplicatedFileChecksumHelper helper = new ReplicatedFileChecksumHelper(
        volume, bucket, "dummy", 10, rpcClient, client);

    helper.compute();
    FileChecksum fileChecksum = helper.getFileChecksum();
    assertTrue(fileChecksum instanceof MD5MD5CRC32GzipFileChecksum);
    assertEquals(1, helper.getKeyLocationInfos().size());
  }

  /*private void invokeXceiverClientGetBlock(XceiverClientSpi client)
      throws IOException {
    // TODO: use ContainerTestHelper.getBlockRequest() instead.

    ContainerProtocolCalls.getBlock(client,
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(1)
            .setLocalID(1)
            .setBlockCommitSequenceId(1)
            .build(), null);
  }*/

  private XceiverClientReply buildValidResponse() {
    ContainerProtos.ContainerCommandResponseProto resp =
        ContainerProtos.ContainerCommandResponseProto.newBuilder()
            .setCmdType(ContainerProtos.Type.GetBlock)
            .setResult(ContainerProtos.Result.SUCCESS).build();
    final CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
        replyFuture = new CompletableFuture<>();
    replyFuture.complete(resp);
    return new XceiverClientReply(replyFuture);
  }
}