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

package org.apache.hadoop.ozone.shell.bucket;

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.acl.AclHandler;
import org.apache.hadoop.ozone.shell.acl.GetAclHandler;
import picocli.CommandLine;

/**
 * Get ACL of bucket.
 */
@CommandLine.Command(name = AclHandler.GET_ACL_NAME,
    description = AclHandler.GET_ACL_DESC)
public class GetAclBucketHandler extends GetAclHandler {

  @CommandLine.Mixin
  private BucketUri address;

  @CommandLine.Option(names = {"--source"},
      defaultValue = "false",
      description = "Display source bucket ACLs if --source=true," +
          " default false to display ACLs of the link bucket itself.")
  private boolean getSourceAcl;

  @Override
  protected OzoneAddress getAddress() {
    return address.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneObj obj) throws IOException {
    if (getSourceAcl) {
      obj = getSourceObj(client, obj);
    }
    super.execute(client, obj);
  }

  private OzoneObj getSourceObj(OzoneClient client, OzoneObj obj)
      throws IOException {
    OzoneBucket bucket = client.getObjectStore()
        .getVolume(obj.getVolumeName())
        .getBucket(obj.getBucketName());
    if (!bucket.isLink()) {
      return obj;
    }
    obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucket.getSourceBucket())
        .setVolumeName(bucket.getSourceVolume())
        .setKeyName(obj.getKeyName())
        .setResType(obj.getResourceType())
        .setStoreType(obj.getStoreType())
        .build();
    return getSourceObj(client, obj);
  }
}
