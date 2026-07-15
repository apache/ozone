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

package org.apache.hadoop.ozone.s3.awssdk;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;

class TestS3SDKWithRatisStreaming extends OzoneS3SDKTests {
  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = super.createOzoneConfig();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED, true);
    // Ensure that all writes use datastream
    conf.set(OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD, "0MB");
    return conf;
  }
}
