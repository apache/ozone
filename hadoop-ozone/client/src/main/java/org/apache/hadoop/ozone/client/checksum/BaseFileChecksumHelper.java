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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
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
  private ClientProtocol rpcClient;

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
      long length, ClientProtocol rpcClient) throws IOException {

    this.volume = volume;
    this.bucket = bucket;
    this.keyName = keyName;
    this.length = length;
    this.rpcClient = rpcClient;
    this.xceiverClientFactory =
        ((RpcClient)rpcClient).getXceiverClientManager();
    if (this.length > 0) {
      fetchBlocks();
    }
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

  /**
   * Request the blocks created in the most recent version from Ozone Manager.
   *
   * @throws IOException
   */
  private void fetchBlocks() throws IOException {
    OzoneManagerProtocol ozoneManagerClient =
        getRpcClient().getOzoneManagerClient();
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
     * magic entry that matches the md5 of a 32 byte zero-padded byte array.
     */
    if (keyLocationInfos == null || keyLocationInfos.isEmpty()) {
      // Explicitly specified here in case the default DataOutputBuffer
      // buffer length value is changed in future.
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
  protected abstract void checksumBlocks() throws IOException;

  /**
   * Make final file checksum result given the per-block or per-block-group
   * checksums collected into getBlockChecksumBuf().
   */
  private FileChecksum makeFinalResult() throws IOException {
    // TODO: support composite CRC
    return makeMd5CrcResult();
  }

  private FileChecksum makeMd5CrcResult() {
    // TODO: support CRC32C
    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(getBlockChecksumBuf().getData());
    // assume CRC32 for now
    return new MD5MD5CRC32GzipFileChecksum(getBytesPerCRC(),
        crcPerBlock, fileMD5);
  }

  public FileChecksum getFileChecksum() {
    return fileChecksum;
  }

}
