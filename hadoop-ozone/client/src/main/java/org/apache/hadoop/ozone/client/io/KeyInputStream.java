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

package org.apache.hadoop.ozone.client.io;

import static java.util.stream.Collectors.groupingBy;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.apache.hadoop.hdds.scm.storage.MultipartInputStream;
import org.apache.hadoop.hdds.scm.storage.PartInputStream;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
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
      Function<OmKeyInfo, OmKeyInfo> retryFunction,
      BlockInputStreamFactory blockStreamFactory,
      OzoneClientConfig config) throws IOException {
    boolean isHsyncFile = keyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID);
    List<BlockExtendedInputStream> partStreams = new ArrayList<>();
    for (int i = 0; i < blockInfos.size(); i++) {
      OmKeyLocationInfo omKeyLocationInfo = blockInfos.get(i);
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
      Function<BlockID, BlockLocationInfo> retry;
      if (retryFunction != null) {
        retry = keyBlockID -> {
          OmKeyInfo newKeyInfo = retryFunction.apply(keyInfo);
          return getBlockLocationInfo(newKeyInfo,
              omKeyLocationInfo.getBlockID());
        };
      } else {
        retry = null;
      }

      if (i == (blockInfos.size() - 1) && isHsyncFile) {
        // block is under construction
        omKeyLocationInfo.setUnderConstruction(true);
      }

      BlockExtendedInputStream stream =
          blockStreamFactory.create(
              keyInfo.getReplicationConfig(),
              omKeyLocationInfo,
              omKeyLocationInfo.getPipeline(),
              omKeyLocationInfo.getToken(),
              xceiverClientFactory,
              retry,
              config);
      partStreams.add(stream);
    }
    return partStreams;
  }

  private static BlockLocationInfo getBlockLocationInfo(OmKeyInfo newKeyInfo,
      BlockID blockID) {
    List<OmKeyLocationInfo> collect =
        newKeyInfo.getLatestVersionLocations()
            .getLocationList()
            .stream()
            .filter(l -> l.getBlockID().equals(blockID))
            .collect(Collectors.toList());
    if (CollectionUtils.isNotEmpty(collect)) {
      return collect.get(0);
    } else {
      return null;
    }
  }

  private static LengthInputStream getFromOmKeyInfo(
      OmKeyInfo keyInfo,
      XceiverClientFactory xceiverClientFactory,
      Function<OmKeyInfo, OmKeyInfo> retryFunction,
      BlockInputStreamFactory blockStreamFactory,
      List<OmKeyLocationInfo> locationInfos,
      OzoneClientConfig config) throws IOException {
    List<BlockExtendedInputStream> streams = createStreams(keyInfo,
        locationInfos, xceiverClientFactory, retryFunction,
        blockStreamFactory, config);
    KeyInputStream keyInputStream =
        new KeyInputStream(keyInfo.getKeyName(), streams);
    return new LengthInputStream(keyInputStream, keyInputStream.getLength());
  }

  /**
   * For each block in keyInfo, add a BlockInputStream to blockStreams.
   */
  public static LengthInputStream getFromOmKeyInfo(OmKeyInfo keyInfo,
      XceiverClientFactory xceiverClientFactory,
      Function<OmKeyInfo, OmKeyInfo> retryFunction,
      BlockInputStreamFactory blockStreamFactory,
      OzoneClientConfig config) throws IOException {

    List<OmKeyLocationInfo> keyLocationInfos = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();

    return getFromOmKeyInfo(keyInfo, xceiverClientFactory,
        retryFunction, blockStreamFactory, keyLocationInfos, config);
  }

  public static List<LengthInputStream> getStreamsFromKeyInfo(OmKeyInfo keyInfo,
      XceiverClientFactory xceiverClientFactory,
      Function<OmKeyInfo, OmKeyInfo> retryFunction,
      BlockInputStreamFactory blockStreamFactory,
      OzoneClientConfig config) throws IOException {

    List<OmKeyLocationInfo> keyLocationInfos = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly();

    // Iterate through each block info in keyLocationInfos and assign it the
    // corresponding part in the partsToBlockMap.
    Map<Integer, List<OmKeyLocationInfo>> partsToBlocksMap =
        keyLocationInfos.stream()
            .collect(groupingBy(BlockLocationInfo::getPartNumber));

    List<LengthInputStream> lengthInputStreams = new ArrayList<>();
    // Create a KeyInputStream for each part.
    for (List<OmKeyLocationInfo> locationInfo : partsToBlocksMap.values()) {
      lengthInputStreams.add(getFromOmKeyInfo(keyInfo, xceiverClientFactory,
          retryFunction, blockStreamFactory, locationInfo,
          config));
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
              + "length=%d position=%d numBytesToRead=%d numBytesRead=%d",
          ((BlockExtendedInputStream) stream).getBlockID(), stream.getLength(),
          stream.getPos(), numBytesToRead, numBytesRead));
    }
  }

  @Override
  @VisibleForTesting
  public List<BlockExtendedInputStream> getPartStreams() {
    return (List<BlockExtendedInputStream>) super.getPartStreams();
  }
}
