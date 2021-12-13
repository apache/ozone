package org.apache.hadoop.ozone.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class BaseFileChecksumHelper {
  static final Logger LOG =
      LoggerFactory.getLogger(BaseFileChecksumHelper.class);

  //private final String src;

  private OzoneVolume volume;
  private OzoneBucket bucket;
  private String keyName;

  private final long length;
  protected RpcClient rpcClient;
  protected XceiverClientSpi xceiverClientSpi;

  //private final Options.ChecksumCombineMode combineMode;
  //private final BlockChecksumType blockChecksumType;
  protected final DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();

  private FileChecksum fileChecksum;
  protected List<OmKeyLocationInfo> keyLocationInfos;
  private int timeout;
  private long remaining = 0L;

  private int bytesPerCRC = -1;
  //private DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
  private long crcPerBlock = 0;
  private boolean isRefetchBlocks = false;
  private int lastRetriedIndex = -1;

  // initialization
  BaseFileChecksumHelper(
      OzoneVolume volume, OzoneBucket bucket, String keyName,
      long length,
      //Options.ChecksumCombineMode checksumCombineMode,
      RpcClient rpcClient,
      XceiverClientSpi xceiverClientGrpc) throws IOException {

    //this.src = src;
    this.volume = volume;
    this.bucket = bucket;
    this.keyName = keyName;
    this.length = length;
    this.rpcClient = rpcClient;
    this.xceiverClientSpi = xceiverClientGrpc;
    refetchBlocks();
  }

  int getBytesPerCRC() {
    return bytesPerCRC;
  }

  void setBytesPerCRC(int bytesPerCRC) {
    this.bytesPerCRC = bytesPerCRC;
  }

  void refetchBlocks() throws IOException {
    //verifyVolumeName(volumeName);
    //verifyBucketName(bucketName);
    //Preconditions.checkNotNull(keyName);

    OzoneManagerProtocol ozoneManagerClient = rpcClient.getOzoneManagerClient();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume.getName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .setSortDatanodesInPipeline(true)
        .setLatestVersionLocation(true)
        .build();
    OmKeyInfo keyInfo = ozoneManagerClient.lookupKey(keyArgs);

    // use OmKeyArgs to call Om.lookup() and get OmKeyInfo
    keyLocationInfos = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();
  }

  public void compute() throws IOException {
    /**
     * request length is 0 or the file is empty, return one with the
     * magic entry that matches what previous hdfs versions return.
     */
    if (keyLocationInfos == null || keyLocationInfos.isEmpty()) {
      // Explicitly specified here in case the default DataOutputBuffer
      // buffer length value is changed in future. This matters because the
      // fixed value 32 has to be used to repeat the magic value for previous
      // HDFS version.
      final int lenOfZeroBytes = 32;
      byte[] emptyBlockMd5 = new byte[lenOfZeroBytes];
      MD5Hash fileMD5 = MD5Hash.digest(emptyBlockMd5);
      fileChecksum =  new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
    } else {
      checksumBlocks();
      fileChecksum = makeFinalResult();
    }
  }

  /**
   * Compute block checksums block by block and append the raw bytes of the
   * block checksums into getBlockChecksumBuf().
   *
   * @throws IOException
   */
  abstract void checksumBlocks() throws IOException;

  /**
   * Make final file checksum result given the per-block or per-block-group
   * checksums collected into getBlockChecksumBuf().
   */
  FileChecksum makeFinalResult() throws IOException {
      return makeMd5CrcResult();
  }

  FileChecksum makeMd5CrcResult() {
    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(blockChecksumBuf.getData());
    // assume CRC32 for now
    return new MD5MD5CRC32GzipFileChecksum(bytesPerCRC,
        crcPerBlock, fileMD5);
  }

  public FileChecksum getFileChecksum() {
    return fileChecksum;
  }

}
