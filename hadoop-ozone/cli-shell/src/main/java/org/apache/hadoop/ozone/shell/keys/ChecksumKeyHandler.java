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

package org.apache.hadoop.ozone.shell.keys;

import static org.apache.hadoop.ozone.client.OzoneClientUtils.getFileChecksumWithCombineMode;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import java.io.IOException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

/**
 * Class to display checksum information about an existing key.
 */
@CommandLine.Command(name = "checksum",
    description = "returns checksum information about an existing key")
public class ChecksumKeyHandler extends KeyHandler {

  @CommandLine.Option(
      names = {"-c", "--combine-mode"},
      description = "Method of combining the chunk checksums. Valid values are "
          + "COMPOSITE_CRC and MD5MD5CRC. Defaults to COMPOSITE_CRC.")
  private OzoneClientConfig.ChecksumCombineMode mode =
      OzoneClientConfig.ChecksumCombineMode.COMPOSITE_CRC;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    ChecksumInfo checksumInfo = new ChecksumInfo(address, client, mode);
    printObjectAsJson(checksumInfo);
  }

  /**
   * Wrapper to the checksum computer to allow it to be override in tests.
   */
  protected FileChecksum getFileChecksum(OzoneVolume vol, OzoneBucket bucket,
      String keyName, long dataSize, ClientProtocol clientProxy)
      throws IOException {
    return getFileChecksumWithCombineMode(vol, bucket, keyName, dataSize, mode,
        clientProxy);
  }

  /**
   * Wrapper class to allow checksum information to be printed as JSON.
   */
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  private class ChecksumInfo {

    private final String volumeName;
    private final String bucketName;
    private final String name;
    private final long dataSize;
    private final String algorithm;
    private final String checksum;

    ChecksumInfo(OzoneAddress address, OzoneClient client,
        OzoneClientConfig.ChecksumCombineMode mode) throws IOException {
      volumeName = address.getVolumeName();
      bucketName = address.getBucketName();
      name = address.getKeyName();

      OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
      OzoneBucket bucket = vol.getBucket(bucketName);
      OzoneKeyDetails key = bucket.getKey(name);
      dataSize = key.getDataSize();

      FileChecksum fileChecksum = getFileChecksum(vol, bucket,
          name, dataSize, client.getObjectStore().getClientProxy());

      this.algorithm = fileChecksum.getAlgorithmName();
      this.checksum = javax.xml.bind.DatatypeConverter.printHexBinary(
          fileChecksum.getBytes());
    }
  }
}
