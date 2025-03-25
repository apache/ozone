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

package org.apache.hadoop.ozone.om.helpers;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.OzoneAclInfo;

/**
 * Helper class for ozone acls operations.
 * This class is used by storage proto only.
 */
final class OzoneAclStorageUtil {
  /**
   * Private constructor.
   */
  private OzoneAclStorageUtil() {
  }

  /**
   * Convert a list of OzoneAcl(java) to list of OzoneAclInfo(protoc).
   * @param protoAcls
   * @return list of OzoneAclInfo.
   */
  public static List<OzoneAclInfo> toProtobuf(List<OzoneAcl> protoAcls) {
    List<OzoneAclInfo> ozoneAclInfos = new ArrayList<>();
    for (OzoneAcl acl : protoAcls) {
      ozoneAclInfos.add(OzoneAclStorage.toProtobuf(acl));
    }
    return ozoneAclInfos;
  }

  /**
   * Convert a list of OzoneAclInfo(protoc) to list of OzoneAcl(java).
   * @param protoAcls
   * @return list of OzoneAcl.
   */
  public static List<OzoneAcl> fromProtobuf(List<OzoneAclInfo> protoAcls) {
    List<OzoneAcl> ozoneAcls = new ArrayList<>();
    for (OzoneAclInfo aclInfo : protoAcls) {
      ozoneAcls.add(OzoneAclStorage.fromProtobuf(aclInfo));
    }
    return ozoneAcls;
  }

}
