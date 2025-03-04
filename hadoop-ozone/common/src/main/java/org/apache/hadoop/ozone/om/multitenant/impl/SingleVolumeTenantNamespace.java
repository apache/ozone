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

package org.apache.hadoop.ozone.om.multitenant.impl;

import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

/**
 * Implements BucketNameSpace which allows exactly ONE VOLUME.
 */
public class SingleVolumeTenantNamespace implements BucketNameSpace {
  public static final String BUCKET_NS_PREFIX = "BucketNS-";
  private final String bucketNameSpaceID;
  private final List<OzoneObj> nsObjects;

  public SingleVolumeTenantNamespace(String id) {
    this(id, id);
  }

  public SingleVolumeTenantNamespace(String id, String volumeName) {
    this.bucketNameSpaceID = BUCKET_NS_PREFIX + id;
    this.nsObjects = ImmutableList.of(new OzoneObjInfo.Builder()
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .setVolumeName(volumeName).build());
  }

  @Override
  public String getBucketNameSpaceID() {
    return bucketNameSpaceID;
  }

  @Override
  public List<OzoneObj> getBucketNameSpaceObjects() {
    return nsObjects;
  }

  @Override
  public void addBucketNameSpaceObject(OzoneObj e) {
    throw new UnsupportedOperationException("Cannot add an object to a single" +
        " SingleVolumeTenantNamespace.");
  }

  @Override
  public SpaceUsageSource getSpaceUsage() {
    return null;
  }

  @Override
  public void setQuota(OzoneQuota quota) {

  }

  @Override
  public OzoneQuota getQuota() {
    return null;
  }
}
