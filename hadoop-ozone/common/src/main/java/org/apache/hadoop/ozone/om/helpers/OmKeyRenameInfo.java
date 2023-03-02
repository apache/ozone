/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;


import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RepeatedString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Argument for renamedKeyTable. Helps to store List<String> which represents
 * all the renames that happened to particular key in between snapshots.
 */
public class OmKeyRenameInfo {
  private List<String> omKeyRenameInfoList;

  public OmKeyRenameInfo(List<String> omKeyRenameInfoList) {
    this.omKeyRenameInfoList = omKeyRenameInfoList;
  }

  public OmKeyRenameInfo(String keyRenameInfo) {
    this.omKeyRenameInfoList = new ArrayList<>();
    this.omKeyRenameInfoList.add(keyRenameInfo);
  }

  public void addOmKeyRenameInfo(String keyRenameInfo) {
    this.omKeyRenameInfoList.add(keyRenameInfo);
  }

  public List<String> getOmKeyRenameInfoList() {
    return omKeyRenameInfoList;
  }

  public List<String> cloneOmKeyRenameInfoList() {
    return new ArrayList<>(omKeyRenameInfoList);
  }


  public static OmKeyRenameInfo getFromProto(RepeatedString
      repeatedString) throws IOException {
    List<String> list = new ArrayList<>(repeatedString.getKeyNameList());
    return new OmKeyRenameInfo.Builder().setOmKeyRenameList(list).build();
  }

  public RepeatedString getProto() {
    List<String> list = new ArrayList<>(cloneOmKeyRenameInfoList());

    RepeatedString.Builder builder = RepeatedString.newBuilder()
        .addAllKeyName(list);
    return builder.build();
  }

  public OmKeyRenameInfo copyObject() {
    return new OmKeyRenameInfo(new ArrayList<>(omKeyRenameInfoList));
  }

  /**
   * Builder of OmKeyRenameInfo.
   */
  public static class Builder {
    private List<String> omKeyRenameList;

    public Builder() { }

    public OmKeyRenameInfo.Builder setOmKeyRenameList(List<String> stringList) {
      this.omKeyRenameList = stringList;
      return this;
    }

    public OmKeyRenameInfo build() {
      return new OmKeyRenameInfo(omKeyRenameList);
    }
  }

}
