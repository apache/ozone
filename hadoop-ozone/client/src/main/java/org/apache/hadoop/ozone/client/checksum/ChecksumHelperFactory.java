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

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

/**
 * Class to create BaseFileChecksumHelper based on the Replication Type.
 */
public final class ChecksumHelperFactory {

  private ChecksumHelperFactory() {
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static BaseFileChecksumHelper getChecksumHelper(
      HddsProtos.ReplicationType replicationType, OzoneVolume volume,
      OzoneBucket bucket, String keyName, long length,
      OzoneClientConfig.ChecksumCombineMode combineMode,
      ClientProtocol rpcClient, OmKeyInfo keyInfo) throws IOException {

    if (replicationType == HddsProtos.ReplicationType.EC) {
      return new ECFileChecksumHelper(volume, bucket, keyName, length,
          combineMode, rpcClient, keyInfo);
    } else {
      return new ReplicatedFileChecksumHelper(volume, bucket, keyName, length,
          combineMode, rpcClient, keyInfo);
    }
  }
}
