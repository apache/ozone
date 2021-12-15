package org.apache.hadoop.ozone.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * The base class to support file checksum.
 */
public abstract class BaseFileChecksumHelper {
  static final Logger LOG =
      LoggerFactory.getLogger(BaseFileChecksumHelper.class);

  private OzoneVolume volume;
  private OzoneBucket bucket;
  private String keyName;

  private final long length;
  protected RpcClient rpcClient;
  protected XceiverClientFactory xceiverClientFactory;

  //private final Options.ChecksumCombineMode combineMode;
  //private final BlockChecksumType blockChecksumType;
  protected final DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();

  private FileChecksum fileChecksum;
  protected List<OmKeyLocationInfo> keyLocationInfos;

  private int bytesPerCRC = -1;
  //private DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
  private long crcPerBlock = 0;

  // initialization
  BaseFileChecksumHelper(
      OzoneVolume volume, OzoneBucket bucket, String keyName,
      long length,
      //Options.ChecksumCombineMode checksumCombineMode,
      RpcClient rpcClient,
      XceiverClientSpi xceiverClientGrpc) throws IOException {

    this.volume = volume;
    this.bucket = bucket;
    this.keyName = keyName;
    this.length = length;
    this.rpcClient = rpcClient;
    this.xceiverClientFactory = rpcClient.getXeiverClientManager();
    refetchBlocks();
  }

  int getBytesPerCRC() {
    return bytesPerCRC;
  }

  void setBytesPerCRC(int bytesPerCRC) {
    this.bytesPerCRC = bytesPerCRC;
  }

  /**
   * Request the blocks created in the most recent version from Ozone Manager.
   *
   * @throws IOException
   */
  void refetchBlocks() throws IOException {
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

  /**
   * Compute file checksum given the list of chunk checksums requested earlier.
   * @throws IOException
   */
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

  @VisibleForTesting
  List<OmKeyLocationInfo> getKeyLocationInfos() {
    return keyLocationInfos;
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
    // TODO: support composite CRC
    return makeMd5CrcResult();
  }

  FileChecksum makeMd5CrcResult() {
    // TODO: support CRC32C
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
