/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.checksum;

import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
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
  private OmKeyInfo keyInfo;

  private OzoneVolume volume;
  private OzoneBucket bucket;
  private String keyName;
  private final long length;

  private ClientProtocol rpcClient;
  private OzoneClientConfig.ChecksumCombineMode combineMode;
  private ContainerProtos.ChecksumType checksumType;

  private final DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();
  private XceiverClientFactory xceiverClientFactory;
  private FileChecksum fileChecksum;
  private List<OmKeyLocationInfo> keyLocationInfos;
  private long remaining = 0L;
  private int bytesPerCRC = -1;
  private long crcPerBlock = 0;

  // initialization
  BaseFileChecksumHelper(
      OzoneVolume volume, OzoneBucket bucket, String keyName,
      long length,
      OzoneClientConfig.ChecksumCombineMode checksumCombineMode,
      ClientProtocol rpcClient) throws IOException {

    this.volume = volume;
    this.bucket = bucket;
    this.keyName = keyName;
    this.length = length;
    this.combineMode = checksumCombineMode;
    this.rpcClient = rpcClient;
    this.xceiverClientFactory =
        ((RpcClient)rpcClient).getXceiverClientManager();
    if (this.length > 0) {
      fetchBlocks();
    }
  }

  public BaseFileChecksumHelper(OzoneVolume volume, OzoneBucket bucket,
      String keyName, long length,
      OzoneClientConfig.ChecksumCombineMode checksumCombineMode,
      ClientProtocol rpcClient, OmKeyInfo keyInfo) throws IOException {
    this(volume, bucket, keyName, length, checksumCombineMode, rpcClient);
    this.keyInfo = keyInfo;
  }

  protected String getSrc() {
    return "Volume: " + volume.getName() + " Bucket: " + bucket.getName() + " "
        + keyName;
  }

  protected long getLength() {
    return length;
  }

  protected ClientProtocol getRpcClient() {
    return rpcClient;
  }

  protected OzoneClientConfig.ChecksumCombineMode getCombineMode() {
    return combineMode;
  }

  protected ContainerProtos.ChecksumType getChecksumType() {
    return checksumType;
  }

  protected XceiverClientFactory getXceiverClientFactory() {
    return xceiverClientFactory;
  }

  protected DataOutputBuffer getBlockChecksumBuf() {
    return blockChecksumBuf;
  }

  protected List<OmKeyLocationInfo> getKeyLocationInfoList() {
    return keyLocationInfos;
  }

  protected long getRemaining() {
    return remaining;
  }

  protected void setRemaining(long remaining) {
    this.remaining = remaining;
  }

  int getBytesPerCRC() {
    return bytesPerCRC;
  }

  protected void setBytesPerCRC(int bytesPerCRC) {
    this.bytesPerCRC = bytesPerCRC;
  }

  protected void setChecksumType(ContainerProtos.ChecksumType type) {
    checksumType = type;
  }

  /**
   * Request the blocks created in the most recent version from Ozone Manager.
   *
   * @throws IOException
   */
  private void fetchBlocks() throws IOException {
    if (keyInfo == null) {
      OzoneManagerProtocol ozoneManagerClient =
          getRpcClient().getOzoneManagerClient();
      OmKeyArgs keyArgs =
          new OmKeyArgs.Builder().setVolumeName(volume.getName())
              .setBucketName(bucket.getName()).setKeyName(keyName)
              .setRefreshPipeline(true).setSortDatanodesInPipeline(true)
              .setLatestVersionLocation(true).build();
      keyInfo = ozoneManagerClient.lookupKey(keyArgs);
    }

    if (keyInfo.getReplicationConfig()
        .getReplicationType() == HddsProtos.ReplicationType.EC) {
      return;
    }

    if (keyInfo.getFileChecksum() != null &&
        isFullLength(keyInfo.getDataSize())) {
      // if the checksum is cached in OM, and we request the checksum of
      // the full length.
      fileChecksum = keyInfo.getFileChecksum();
    }

    // use OmKeyArgs to call Om.lookup() and get OmKeyInfo
    keyLocationInfos = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();
  }

  /**
   * Return true if the requested length is longer than the file length
   * (dataSize).
   *
   * @param dataSize file length
   * @return
   */
  private boolean isFullLength(long dataSize) {
    return this.length >= dataSize;
  }

  /**
   * Compute file checksum given the list of chunk checksums requested earlier.
   *
   * Skip computation if the already computed, or if the OmKeyInfo of the key
   * in OM has pre-computed checksum.
   * @throws IOException
   */
  public void compute() throws IOException {
    if (fileChecksum != null) {
      LOG.debug("Checksum is available. Skip computing it.");
      return;
    }
    /**
     * request length is 0 or the file is empty, return one with the
     * magic entry that matches the md5 of a 32 byte zero-padded byte array.
     */
    if (keyLocationInfos == null || keyLocationInfos.isEmpty()) {
      // Explicitly specified here in case the default DataOutputBuffer
      // buffer length value is changed in future.
      final int lenOfZeroBytes = 32;
      byte[] emptyBlockMd5 = new byte[lenOfZeroBytes];
      MD5Hash fileMD5 = MD5Hash.digest(emptyBlockMd5);
      fileChecksum = new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
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
  protected abstract void checksumBlocks() throws IOException;

  /**
   * Make final file checksum result given the per-block or per-block-group
   * checksums collected into getBlockChecksumBuf().
   */
  private FileChecksum makeFinalResult() throws IOException {
    switch (getCombineMode()) {
    case MD5MD5CRC:
      return makeMd5CrcResult();
    case COMPOSITE_CRC:
      return makeCompositeCrcResult();
    default:
      throw new IOException("Unknown ChecksumCombineMode: " +
          getCombineMode());
    }
  }

  private FileChecksum makeMd5CrcResult() {
    // TODO: support CRC32C
    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(getBlockChecksumBuf().getData());
    // assume CRC32 for now
    switch (getChecksumType()) {
    case CRC32:
      return new MD5MD5CRC32GzipFileChecksum(getBytesPerCRC(),
          crcPerBlock, fileMD5);
    case CRC32C:
      return new MD5MD5CRC32CastagnoliFileChecksum(getBytesPerCRC(),
          crcPerBlock, fileMD5);
    default:
      throw new IllegalArgumentException("unexpected checksum type "
          + getChecksumType());
    }

  }

  DataChecksum.Type toHadoopChecksumType() {
    switch (checksumType) {
    case CRC32:
      return DataChecksum.Type.CRC32;
    case CRC32C:
      return DataChecksum.Type.CRC32C;
    default:
      throw new IllegalArgumentException("unsupported checksum type");
    }
  }

  FileChecksum makeCompositeCrcResult() throws IOException {
    long blockSizeHint = 0;
    if (keyLocationInfos.size() > 0) {
      blockSizeHint = keyLocationInfos.get(0).getLength();
    }
    CrcComposer crcComposer =
        CrcComposer.newCrcComposer(toHadoopChecksumType(), blockSizeHint);
    byte[] blockChecksumBytes = blockChecksumBuf.getData();

    for (int i = 0; i < keyLocationInfos.size(); ++i) {
      OmKeyLocationInfo block = keyLocationInfos.get(i);
      // For every LocatedBlock, we expect getBlockSize()
      // to accurately reflect the number of file bytes digested in the block
      // checksum.
      int blockCrc = CrcUtil.readInt(blockChecksumBytes, i * 4);

      crcComposer.update(blockCrc, block.getLength());
      LOG.debug(
          "Added blockCrc 0x{} for block index {} of size {}",
          Integer.toString(blockCrc, 16), i, block.getLength());
    }

    int compositeCrc = CrcUtil.readInt(crcComposer.digest(), 0);
    return new CompositeCrcFileChecksum(
        compositeCrc, toHadoopChecksumType(), bytesPerCRC);
  }

  public FileChecksum getFileChecksum() {
    return fileChecksum;
  }

}
