/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the result of listOpenFiles. It has a list of
 * {@link OpenKeySession} and a boolean flag indicating if there
 * are more entries that are not fetched after the current batch of result.
 */
public class ListOpenFilesResult {
  /**
   * List of open files. Each has client ID and OmKeyInfo.
   */
  private final List<OpenKeySession> openKeySessionList;
  /**
   * True if there are more entries after this batch under the given path.
   */
  private final boolean hasMore;
  /**
   * Number of total open files globally.
   */
  private final long globalTotal;

  public ListOpenFilesResult(List<OpenKeySession> openKeySessionList,
                             boolean hasMore, long globalTotal) {
    this.openKeySessionList = openKeySessionList;
    this.hasMore = hasMore;
    this.globalTotal = globalTotal;
  }

  public ListOpenFilesResult(List<Long> clientIDsList,
                             List<KeyInfo> keyInfosList,
                             boolean hasMore, long globalTotal)
      throws IOException {
    this.openKeySessionList = getOpenKeySessionListFromPB(clientIDsList,
        keyInfosList);
    this.hasMore = hasMore;
    this.globalTotal = globalTotal;
  }

  private List<OpenKeySession> getOpenKeySessionListFromPB(
      List<Long> clientIDsList, List<KeyInfo> keyInfosList)
      throws IOException {

    Preconditions.checkArgument(clientIDsList.size() == keyInfosList.size(),
        "clientIDsList size (" + clientIDsList.size() + ") should be " +
            "the same as keyInfosList's (" + keyInfosList.size() + ")");

    List<OpenKeySession> res = new ArrayList<>();

    for (int i = 0; i < clientIDsList.size(); i++) {
      OmKeyInfo omKeyInfo = OmKeyInfo.getFromProtobuf(keyInfosList.get(i));
      res.add(new OpenKeySession(clientIDsList.get(i),
          omKeyInfo,
          omKeyInfo.getLatestVersionLocations().getVersion()));
    }
    return res;
  }

  public List<OpenKeySession> getOpenFiles() {
    return openKeySessionList;
  }

  public boolean hasMore() {
    return hasMore;
  }

  public long getGlobalTotal() {
    return globalTotal;
  }
}
