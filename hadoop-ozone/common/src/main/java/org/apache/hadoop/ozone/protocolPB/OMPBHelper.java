/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclRights;

/**
 * Utilities for converting protobuf classes.
 */
public final class OMPBHelper {

  private OMPBHelper() {
    /** Hidden constructor */
  }

  /**
   * Converts OzoneAcl into protobuf's OzoneAclInfo.
   * @return OzoneAclInfo
   */
  public static OzoneAclInfo convertOzoneAcl(OzoneAcl acl) {
    OzoneAclInfo.OzoneAclType aclType;
    switch(acl.getType()) {
    case USER:
      aclType = OzoneAclType.USER;
      break;
    case GROUP:
      aclType = OzoneAclType.GROUP;
      break;
    case WORLD:
      aclType = OzoneAclType.WORLD;
      break;
    default:
      throw new IllegalArgumentException("ACL type is not recognized");
    }
    OzoneAclInfo.OzoneAclRights aclRights;
    switch(acl.getRights()) {
    case READ:
      aclRights = OzoneAclRights.READ;
      break;
    case WRITE:
      aclRights = OzoneAclRights.WRITE;
      break;
    case READ_WRITE:
      aclRights = OzoneAclRights.READ_WRITE;
      break;
    default:
      throw new IllegalArgumentException("ACL right is not recognized");
    }

    return OzoneAclInfo.newBuilder().setType(aclType)
        .setName(acl.getName())
        .setRights(aclRights)
        .build();
  }

  /**
   * Converts protobuf's OzoneAclInfo into OzoneAcl.
   * @return OzoneAcl
   */
  public static OzoneAcl convertOzoneAcl(OzoneAclInfo aclInfo) {
    OzoneAcl.OzoneACLType aclType;
    switch(aclInfo.getType()) {
    case USER:
      aclType = OzoneAcl.OzoneACLType.USER;
      break;
    case GROUP:
      aclType = OzoneAcl.OzoneACLType.GROUP;
      break;
    case WORLD:
      aclType = OzoneAcl.OzoneACLType.WORLD;
      break;
    default:
      throw new IllegalArgumentException("ACL type is not recognized");
    }
    OzoneAcl.OzoneACLRights aclRights;
    switch(aclInfo.getRights()) {
    case READ:
      aclRights = OzoneAcl.OzoneACLRights.READ;
      break;
    case WRITE:
      aclRights = OzoneAcl.OzoneACLRights.WRITE;
      break;
    case READ_WRITE:
      aclRights = OzoneAcl.OzoneACLRights.READ_WRITE;
      break;
    default:
      throw new IllegalArgumentException("ACL right is not recognized");
    }

    return new OzoneAcl(aclType, aclInfo.getName(), aclRights);
  }
}
