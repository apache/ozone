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

package org.apache.hadoop.fs.ozone.contract;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

/**
 * Filesystem contract for O3FS.
 */
final class OzoneContract extends AbstractOzoneContract {

  OzoneContract(MiniOzoneCluster cluster) {
    super(cluster);
  }

  @Override
  public String getScheme() {
    return OzoneConsts.OZONE_URI_SCHEME;
  }

  @Override
  public Path getTestPath() {
    return new Path("/test");
  }

  @Override
  protected String getRootURI() throws IOException {
    try (OzoneClient client = getCluster().newClient()) {
      BucketLayout layout = getConf().getEnum(OZONE_DEFAULT_BUCKET_LAYOUT, BucketLayout.DEFAULT);
      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, layout);
      return String.format("%s://%s.%s/", getScheme(), bucket.getName(), bucket.getVolumeName());
    }
  }
}
