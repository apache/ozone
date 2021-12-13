package org.apache.hadoop.ozone.client;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestReplicatedFileChecksumHelper {

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
}