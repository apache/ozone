/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.scm.storage.MultipartInputStream;
import org.apache.hadoop.hdds.scm.storage.PartInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintaining a list of BlockInputStream. Read based on offset.
 */
public class KeyInputStream extends MultipartInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(KeyInputStream.class);

  public KeyInputStream(String keyName,
                        List<? extends BlockExtendedInputStream> inputStreams) {
    super(keyName, inputStreams);
  }

  private static List<BlockExtendedInputStream> createStreams(
      OmKeyInfo keyInfo,
      List<OmKeyLocationInfo> blockInfos,
      XceiverClientFactory xceiverClientFactory,
      boolean verifyChecksum,
      Function<OmKeyInfo, OmKeyInfo> retryFunction,
      BlockInputStreamFactory blockStreamFactory) {
    List<BlockExtendedInputStream> partStreams = new ArrayList<>();
    for (OmKeyLocationInfo omKeyLocationInfo : blockInfos) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding stream for accessing {}. The stream will be " +
            "initialized later.", omKeyLocationInfo);
      }
      // We also pass in functional reference which is used to refresh the
      // pipeline info for a given OM Key location info.

      // Append another BlockInputStream to the end of the list. Note that the
      // BlockInputStream is only created here and not initialized. The
      // BlockInputStream is initialized when a read operation is performed on
      // the block for the first time.
      BlockExtendedInputStream stream =
          blockStreamFactory.create(
              keyInfo.getReplicationConfig(),
              omKeyLocationInfo,
              omKeyLocationInfo.getPipeline(),
              omKeyLocationInfo.getToken(),
              verifyChecksum,
              xceiverClientFactory,
              keyBlockID -> {
                OmKeyInfo newKeyInfo = retryFunction.apply(keyInfo);
                return getPipeline(newKeyInfo, omKeyLocationInfo.getBlockID());
              });
      partStreams.add(stream);
    }
    return partStreams;
  }

  private static Pipeline getPipeline(OmKeyInfo newKeyInfo, BlockID blockID) {
    List<OmKeyLocationInfo> collect =
        newKeyInfo.getLatestVersionLocations()
            .getLocationList()
            .stream()
            .filter(l -> l.getBlockID().equals(blockID))
            .collect(Collectors.toList());
    if (CollectionUtils.isNotEmpty(collect)) {
      return collect.get(0).getPipeline();
    } else {
      return null;
    }
  }

  /**
   * For each block in keyInfo, add a BlockInputStream to blockStreams.
   */
  public static LengthInputStream getFromOmKeyInfo(OmKeyInfo keyInfo,
      XceiverClientFactory xceiverClientFactory,
      boolean verifyChecksum,  Function<OmKeyInfo, OmKeyInfo> retryFunction,
      BlockInputStreamFactory blockStreamFactory) {
    List<OmKeyLocationInfo> keyLocationInfos = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();

    List<BlockExtendedInputStream> streams = createStreams(keyInfo,
        keyLocationInfos, xceiverClientFactory, verifyChecksum, retryFunction,
        blockStreamFactory);
    KeyInputStream keyInputStream =
        new KeyInputStream(keyInfo.getKeyName(), streams);

    return new LengthInputStream(keyInputStream, keyInputStream.getLength());
  }

  public static List<LengthInputStream> getStreamsFromKeyInfo(OmKeyInfo keyInfo,
      XceiverClientFactory xceiverClientFactory, boolean verifyChecksum,
      Function<OmKeyInfo, OmKeyInfo> retryFunction,
      BlockInputStreamFactory blockStreamFactory) {
    List<OmKeyLocationInfo> keyLocationInfos = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();

    List<LengthInputStream> lengthInputStreams = new ArrayList<>();

    // Iterate through each block info in keyLocationInfos and assign it the
    // corresponding part in the partsToBlockMap. Also increment each part's
    // length accordingly.
    Map<Integer, List<OmKeyLocationInfo>> partsToBlocksMap = new HashMap<>();
    Map<Integer, Long> partsLengthMap = new HashMap<>();

    for (OmKeyLocationInfo omKeyLocationInfo: keyLocationInfos) {
      int partNumber = omKeyLocationInfo.getPartNumber();

      if (!partsToBlocksMap.containsKey(partNumber)) {
        partsToBlocksMap.put(partNumber, new ArrayList<>());
        partsLengthMap.put(partNumber, 0L);
      }
      // Add Block to corresponding partNumber in partsToBlocksMap
      partsToBlocksMap.get(partNumber).add(omKeyLocationInfo);
      // Update the part length
      partsLengthMap.put(partNumber,
          partsLengthMap.get(partNumber) + omKeyLocationInfo.getLength());
    }

    // Create a KeyInputStream for each part.
    for (Map.Entry<Integer, List<OmKeyLocationInfo>> entry :
        partsToBlocksMap.entrySet()) {
      List<BlockExtendedInputStream> streams = createStreams(keyInfo,
          entry.getValue(), xceiverClientFactory, verifyChecksum, retryFunction,
          blockStreamFactory);
      KeyInputStream keyInputStream =
          new KeyInputStream(keyInfo.getKeyName(), streams);
      lengthInputStreams.add(new LengthInputStream(keyInputStream,
          partsLengthMap.get(entry.getKey())));
    }

    return lengthInputStreams;
  }

  @Override
  protected int getNumBytesToRead(ByteReaderStrategy strategy,
                                  PartInputStream current) throws IOException {
    return (int) Math.min(strategy.getTargetLength(), current.getRemaining());
  }

  @Override
  protected void checkPartBytesRead(int numBytesToRead, int numBytesRead,
                                    PartInputStream stream) throws IOException {
    if (numBytesRead != numBytesToRead) {
      // This implies that there is either data loss or corruption in the
      // chunk entries. Even EOF in the current stream would be covered in
      // this case.
      throw new IOException(String.format("Inconsistent read for blockID=%s "
              + "length=%d numBytesToRead=%d numBytesRead=%d",
          ((BlockExtendedInputStream) stream).getBlockID(), stream.getLength(),
          numBytesToRead, numBytesRead));
    }
  }

  @Override
  @VisibleForTesting
  public List<BlockExtendedInputStream> getPartStreams() {
    return (List<BlockExtendedInputStream>) super.getPartStreams();
  }
}
