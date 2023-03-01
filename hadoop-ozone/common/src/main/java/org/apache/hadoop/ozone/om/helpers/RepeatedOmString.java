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
public class RepeatedOmString {
  private List<String> omStringList;

  public RepeatedOmString(List<String> stringList) {
    this.omStringList = stringList;
  }

  public RepeatedOmString(String string) {
    this.omStringList = new ArrayList<>();
    this.omStringList.add(string);
  }

  public void addOmString(String string) {
    this.omStringList.add(string);
  }

  public List<String> getOmStringList() {
    return omStringList;
  }

  public List<String> cloneOmStringList() {
    return new ArrayList<>(omStringList);
  }


  public static RepeatedOmString getFromProto(RepeatedString
      repeatedString) throws IOException {
    List<String> list = new ArrayList<>(repeatedString.getKeyNameList());
    return new RepeatedOmString.Builder().setOmString(list).build();
  }

  public RepeatedString getProto() {
    List<String> list = new ArrayList<>(cloneOmStringList());

    RepeatedString.Builder builder = RepeatedString.newBuilder()
        .addAllKeyName(list);
    return builder.build();
  }

  public RepeatedOmString copyObject() {
    return new RepeatedOmString(new ArrayList<>(omStringList));
  }

  /**
   * Builder of RepeatedOmString.
   */
  public static class Builder {
    private List<String> omStringList;

    public Builder() { }

    public RepeatedOmString.Builder setOmString(List<String> stringList) {
      this.omStringList = stringList;
      return this;
    }

    public RepeatedOmString build() {
      return new RepeatedOmString(omStringList);
    }
  }

}
