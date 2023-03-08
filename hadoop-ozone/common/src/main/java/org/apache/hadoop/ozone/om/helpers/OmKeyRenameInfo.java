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


import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyRenameInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Argument for renamedKeyTable. Helps to store List<String> which represents
 * all the renames that happened to particular key in between snapshots.
 */
public class OmKeyRenameInfo {
  private List<String> keyNamesList;

  public OmKeyRenameInfo(List<String> keyNamesList) {
    this.keyNamesList = keyNamesList;
  }

  public OmKeyRenameInfo(String keyRenameInfo) {
    this.keyNamesList = new ArrayList<>();
    this.keyNamesList.add(keyRenameInfo);
  }

  public void addOmKeyRenameInfo(String keyRenameInfo) {
    this.keyNamesList.add(keyRenameInfo);
  }

  public List<String> getOmKeyRenameInfoList() {
    return keyNamesList;
  }

  public List<String> cloneOmKeyRenameInfoList() {
    return new ArrayList<>(keyNamesList);
  }


  public static OmKeyRenameInfo getFromProto(KeyRenameInfo
      keyRenameInfo) throws IOException {
    List<String> list = new ArrayList<>(keyRenameInfo.getKeyNamesList());
    return new OmKeyRenameInfo.Builder().setOmKeyRenameList(list).build();
  }

  public KeyRenameInfo getProto() {
    List<String> list = new ArrayList<>(cloneOmKeyRenameInfoList());

    KeyRenameInfo.Builder builder = KeyRenameInfo.newBuilder()
        .addAllKeyNames(list);
    return builder.build();
  }

  public OmKeyRenameInfo copyObject() {
    return new OmKeyRenameInfo(new ArrayList<>(keyNamesList));
  }

  /**
   * Builder of OmKeyRenameInfo.
   */
  public static class Builder {
    private List<String> keyNamesList;

    public Builder() { }

    public OmKeyRenameInfo.Builder setOmKeyRenameList(List<String> stringList) {
      this.keyNamesList = stringList;
      return this;
    }

    public OmKeyRenameInfo build() {
      return new OmKeyRenameInfo(keyNamesList);
    }
  }

}
