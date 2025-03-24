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

import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneAcl.AclScope;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.OzoneAclInfo.OzoneAclScope;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.OzoneAclInfo.OzoneAclType;

/**
 * OzoneAclStorage classes define bucket ACLs used in OZONE.
 * This class is used by storage proto only.
 *
 * ACLs in Ozone follow this pattern.
 * <ul>
 * <li>user:name:rw
 * <li>group:name:rw
 * <li>world::rw
 * </ul>
 */
final class OzoneAclStorage {
  /**
   * Private constructor.
   */
  private OzoneAclStorage() {
  }

  public static OzoneAclInfo toProtobuf(OzoneAcl acl) {
    OzoneAclInfo.Builder builder = OzoneAclInfo.newBuilder()
        .setName(acl.getName())
        .setType(OzoneAclType.valueOf(acl.getType().name()))
        .setAclScope(OzoneAclScope.valueOf(acl.getAclScope().name()))
        .setRights(acl.getAclByteString());
    return builder.build();
  }

  public static OzoneAcl fromProtobuf(OzoneAclInfo protoAcl) {
    BitSet aclRights = BitSet.valueOf(protoAcl.getRights().toByteArray());
    List<IAccessAuthorizer.ACLType> aclTypeList = aclRights.stream()
        .mapToObj(a -> IAccessAuthorizer.ACLType.values()[a])
        .collect(Collectors.toList());
    EnumSet<IAccessAuthorizer.ACLType> aclSet = EnumSet.copyOf(aclTypeList);
    return OzoneAcl.of(ACLIdentityType.valueOf(protoAcl.getType().name()),
        protoAcl.getName(), AclScope.valueOf(protoAcl.getAclScope().name()), aclSet);
  }

}
